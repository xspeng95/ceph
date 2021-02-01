// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_types.h"
#include "librbd/operation/SnapshotCreateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/mirror/snapshot/SetImageStateRequest.h"

#include "include/cpp-bplustree/BpTree.hpp"
#include <fstream>
#include <iostream>
#include "include/cpp-bplustree/BpTree.hpp"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotCreateRequest: "

namespace librbd {
namespace operation {

using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
SnapshotCreateRequest<I>::SnapshotCreateRequest(I &image_ctx,
                                                Context *on_finish,
						const cls::rbd::SnapshotNamespace &snap_namespace,
                                                const std::string &snap_name,
                                                uint64_t journal_op_tid,
                                                bool skip_object_map)
  : Request<I>(image_ctx, on_finish, journal_op_tid),
    m_snap_namespace(snap_namespace), m_snap_name(snap_name),
    m_skip_object_map(skip_object_map), m_ret_val(0), m_snap_id(CEPH_NOSNAP) {
//    m_id_obj_map_snapid_name= util::id_obj_map_snapid_name(m_snap_id);
}

template <typename I>
void SnapshotCreateRequest<I>::send_op() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if (!image_ctx.data_ctx.is_valid()) {
    lderr(cct) << "missing data pool" << dendl;
    this->async_complete(-ENODEV);
    return;
  }

  send_suspend_requests();
}

template <typename I>
void SnapshotCreateRequest<I>::send_suspend_requests() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  // TODO suspend (shrink) resize to ensure consistent RBD mirror
  send_suspend_aio();
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_suspend_requests(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  // TODO
  send_suspend_aio();
  return nullptr;
}

template <typename I>
void SnapshotCreateRequest<I>::send_suspend_aio() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  image_ctx.io_work_queue->block_writes(create_context_callback<
    SnapshotCreateRequest<I>,
    &SnapshotCreateRequest<I>::handle_suspend_aio>(this));
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_suspend_aio(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(*result) << dendl;
    image_ctx.io_work_queue->unblock_writes();
    return this->create_context_finisher(*result);
  }

  send_append_op_event();
  return nullptr;
}

template <typename I>
void SnapshotCreateRequest<I>::send_append_op_event() {
  I &image_ctx = this->m_image_ctx;
  if (!this->template append_op_event<
        SnapshotCreateRequest<I>,
        &SnapshotCreateRequest<I>::handle_append_op_event>(this)) {
    send_allocate_snap_id();
    return;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_append_op_event(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    image_ctx.io_work_queue->unblock_writes();
    lderr(cct) << "failed to commit journal entry: " << cpp_strerror(*result)
               << dendl;
    return this->create_context_finisher(*result);
  }

  send_allocate_snap_id();
  return nullptr;
}

template <typename I>
void SnapshotCreateRequest<I>::send_allocate_snap_id() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  librados::AioCompletion *rados_completion = create_rados_callback<
    SnapshotCreateRequest<I>,
    &SnapshotCreateRequest<I>::handle_allocate_snap_id>(this);
  image_ctx.data_ctx.aio_selfmanaged_snap_create(&m_snap_id, rados_completion);
  rados_completion->release();
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_allocate_snap_id(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << ", "
                << "snap_id=" << m_snap_id << dendl;

  if (*result < 0) {
    save_result(result);
    image_ctx.io_work_queue->unblock_writes();
    lderr(cct) << "failed to allocate snapshot id: " << cpp_strerror(*result)
               << dendl;
    return this->create_context_finisher(*result);
  }
  send_create_image_object_map_snapid();
  send_create_snap();
  return nullptr;
}

template <typename I>
void SnapshotCreateRequest<I>::send_create_snap() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  std::shared_lock owner_locker{image_ctx.owner_lock};
  std::shared_lock image_locker{image_ctx.image_lock};

  // should have been canceled prior to releasing lock
  ceph_assert(image_ctx.exclusive_lock == nullptr ||
              image_ctx.exclusive_lock->is_lock_owner());

  // save current size / parent info for creating snapshot record in ImageCtx
  m_size = image_ctx.size;
  m_parent_info = image_ctx.parent_md;

  librados::ObjectWriteOperation op;
  if (image_ctx.old_format) {
    cls_client::old_snapshot_add(&op, m_snap_id, m_snap_name);
  } else {
    cls_client::snapshot_add(&op, m_snap_id, m_snap_name, m_snap_namespace);
  }

  librados::AioCompletion *rados_completion = create_rados_callback<
    SnapshotCreateRequest<I>,
    &SnapshotCreateRequest<I>::handle_create_snap>(this);
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid,
                                       rados_completion, &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_create_snap(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == -ESTALE) {
    send_allocate_snap_id();
    return nullptr;
  } else if (*result < 0) {
    save_result(result);
    send_release_snap_id();
    return nullptr;
  }
    ofstream ofile;
    ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
    if(!ofile.is_open()){
        cout<<"open file error!";
    }
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::handle_create_snap()\n";
    ofile.close();
  return send_create_object_map();

}

template <typename I>
Context *SnapshotCreateRequest<I>::send_create_object_map() {
  I &image_ctx = this->m_image_ctx;

  image_ctx.image_lock.lock_shared();
  if (image_ctx.object_map == nullptr || m_skip_object_map) {
    image_ctx.image_lock.unlock_shared();

    return send_create_image_state();
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  image_ctx.object_map->snapshot_add(
    m_snap_id, create_context_callback<
      SnapshotCreateRequest<I>,
      &SnapshotCreateRequest<I>::handle_create_object_map>(this));
  image_ctx.image_lock.unlock_shared();
  return nullptr;
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_create_object_map(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << this << " " << __func__ << ": failed to snapshot object map: "
               << cpp_strerror(*result) << dendl;

    update_snap_context();
    image_ctx.io_work_queue->unblock_writes();
    return this->create_context_finisher(*result);
  }
    ofstream ofile;
    ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
    if(!ofile.is_open()){
        cout<<"open file error!";
    }
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::handle_create_object_map()\n";
    ofile.close();

  return send_create_image_state();

}

template <typename I>
Context *SnapshotCreateRequest<I>::send_create_image_state() {
  I &image_ctx = this->m_image_ctx;
  auto mirror_ns = boost::get<cls::rbd::MirrorSnapshotNamespace>(
    &m_snap_namespace);
    ofstream ofile;
    ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
    if(!ofile.is_open()){
        cout<<"open file error!";
    }
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_state()|| before if\n";

  if (mirror_ns == nullptr || !mirror_ns->is_primary()) {
    update_snap_context();
    image_ctx.io_work_queue->unblock_writes();
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_state()|| into if\n";
//    send_create_image_object_map_snapid();
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_state()|| after create_map_snapid\n";
    return this->create_context_finisher(0);
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  auto req = mirror::snapshot::SetImageStateRequest<I>::create(
      &image_ctx, m_snap_id, create_context_callback<
      SnapshotCreateRequest<I>,
      &SnapshotCreateRequest<I>::handle_create_image_state>(this));
  req->send();
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_state()|| after if\n";
    ofile.close();
  return nullptr;
}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_create_image_state(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;
    ofstream ofile;
    ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
    if(!ofile.is_open()){
        cout<<"open file error!";
    }
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::handle_create_image_state()|| do update_snap_context()\n";
  update_snap_context();
  image_ctx.io_work_queue->unblock_writes();
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::handle_create_image_state()|| finish update_snap_context()\n";
  if (*result < 0) {
    lderr(cct) << this << " " << __func__ << ": failed to snapshot object map: "
               << cpp_strerror(*result) << dendl;
    return this->create_context_finisher(*result);
  }

    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::handle_create_image_state()\n";
    ofile.close();

    return this->create_context_finisher(0);

}
//设置每个映射的数据块的名称
template <typename I>
std::string SnapshotCreateRequest<I>::object_map_name(const std::string &image_id,
                                                   uint64_t snap_id) {
    std::string oid(RBD_OBJECT_MAP_SNAPID_PREFIX + image_id);
    if (snap_id != CEPH_NOSNAP) {
        std::stringstream snap_suffix;
        snap_suffix << "." << std::setfill('0') << std::setw(16) << std::hex
                        << snap_id;
        oid += snap_suffix.str();
    }
    return oid;
}
template <typename I>
void SnapshotCreateRequest<I>::send_create_image_object_map_snapid() {
    I &image_ctx=this->m_image_ctx;
    CephContext *cct=image_ctx.cct;
    ldout(cct, 5) << this << " " << __func__ << dendl;

//    std::shared_lock owner_locker{image_ctx.owner_lock};
//    std::shared_lock image_locker{image_ctx.image_lock};
//
//    // should have been canceled prior to releasing lock
//    ceph_assert(image_ctx.exclusive_lock == nullptr ||
//                image_ctx.exclusive_lock->is_lock_owner());

//    Node* tree_head= image_ctx.head_image_bptree.getRootNode();
    ofstream ofile;
    ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
    if(!ofile.is_open()){
        cout<<"open file error!";
    }
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_object_map_snapid()\n";

    librados::ObjectWriteOperation op;
    op.create(true);
    std::string oid(SnapshotCreateRequest<>::object_map_name(image_ctx.id, m_snap_id));


    if(image_ctx.snap_tree_set.empty()){
        //如果是head image的第一个快照，则snap_prev则是自己
        ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_object_map_snapid()|| first snap\n";
        cls_client::set_object_map_snapid(&op,image_ctx.object_count,m_snap_id,m_snap_id);

    } else{
        map<snapid_t,BpTree>::iterator snap_tree_set_lastitem=image_ctx.snap_tree_set.end();
        snapid_t snap_prev=snap_tree_set_lastitem->first;
        cls_client::set_object_map_snapid(&op,image_ctx.object_count,m_snap_id,snap_prev);
    }
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_object_map_snapid()|| after cls_client::set\n";
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_object_map_snapid()|| oid="<<oid<<"\n";
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_object_map_snapid()|| image_id="<<image_ctx.id<<"\n";
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_object_map_snapid()|| m_snap_id="<<m_snap_id<<"\n";

    using klass=SnapshotCreateRequest<I>;
    librados::AioCompletion *comp=
            create_rados_callback<klass,&klass::handle_create_image_object_map_snapid>(this);
    int r=image_ctx.md_ctx.aio_operate(oid,comp,&op);
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_object_map_snapid()|| r="<<r<<"\n";
    ceph_assert(r == 0);
    comp->release();
    ofile<<"come to librbd::operation::SnapshotCreateRequest.cc::send_create_image_object_map_snapid()|| after comp release\n";
    ofile.close();
}
template <typename I>
Context *SnapshotCreateRequest<I>::handle_create_image_object_map_snapid(int *result) {
    I &image_ctx=this->m_image_ctx;
    CephContext *cct=image_ctx.cct;
    ldout(cct, 5) << this << " " << __func__ << ": *result=" << *result << dendl;

    if (*result < 0) {
        lderr(cct) << this << " " << __func__ << ": failed to snapshot object map: "
                   << cpp_strerror(*result) << dendl;
    }

//    return this->create_context_finisher(0);
    return nullptr;
}


template <typename I>
void SnapshotCreateRequest<I>::send_release_snap_id() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  ceph_assert(m_snap_id != CEPH_NOSNAP);

  librados::AioCompletion *rados_completion = create_rados_callback<
    SnapshotCreateRequest<I>,
    &SnapshotCreateRequest<I>::handle_release_snap_id>(this);
  image_ctx.data_ctx.aio_selfmanaged_snap_remove(m_snap_id, rados_completion);
  rados_completion->release();

}

template <typename I>
Context *SnapshotCreateRequest<I>::handle_release_snap_id(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  ceph_assert(m_ret_val < 0);
  *result = m_ret_val;

  image_ctx.io_work_queue->unblock_writes();
  return this->create_context_finisher(m_ret_val);
}

template <typename I>
void SnapshotCreateRequest<I>::update_snap_context() {
  I &image_ctx = this->m_image_ctx;

  std::shared_lock owner_locker{image_ctx.owner_lock};
  std::unique_lock image_locker{image_ctx.image_lock};
  if (image_ctx.old_format) {
    return;
  }

  if (image_ctx.get_snap_info(m_snap_id) != NULL) {
    return;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  // should have been canceled prior to releasing lock
  ceph_assert(image_ctx.exclusive_lock == nullptr ||
              image_ctx.exclusive_lock->is_lock_owner());

  // immediately add a reference to the new snapshot
  utime_t snap_time = ceph_clock_now();
  image_ctx.add_snap(m_snap_namespace, m_snap_name, m_snap_id, m_size,
		     m_parent_info, RBD_PROTECTION_STATUS_UNPROTECTED,
		     0, snap_time);

  // immediately start using the new snap context if we
  // own the exclusive lock
  std::vector<snapid_t> snaps;
  snaps.push_back(m_snap_id);
  snaps.insert(snaps.end(), image_ctx.snapc.snaps.begin(),
               image_ctx.snapc.snaps.end());

  image_ctx.snapc.seq = m_snap_id;
  image_ctx.snapc.snaps.swap(snaps);
  image_ctx.data_ctx.selfmanaged_snap_set_write_ctx(
    image_ctx.snapc.seq, image_ctx.snaps);

  if (!image_ctx.migration_info.empty()) {
    auto it = image_ctx.migration_info.snap_map.find(CEPH_NOSNAP);
    ceph_assert(it != image_ctx.migration_info.snap_map.end());
    ceph_assert(!it->second.empty());
    if (it->second[0] == CEPH_NOSNAP) {
      ldout(cct, 5) << this << " " << __func__
                    << ": updating migration snap_map" << dendl;
      it->second[0] = m_snap_id;
    }
  }
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotCreateRequest<librbd::ImageCtx>;
