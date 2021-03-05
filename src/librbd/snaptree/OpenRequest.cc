//
// Created by xspeng on 2021/2/26.
//
#include "librbd/snaptree/OpenRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ConfigWatcher.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/cache/ObjectCacherObjectDispatch.h"
#include "librbd/cache/WriteAroundObjectDispatch.h"
#include "librbd/cache/ParentCacheObjectDispatch.cc"
#include "librbd/image/CloseRequest.h"
#include "librbd/image/RefreshRequest.h"
#include "librbd/image/SetSnapRequest.h"
#include "librbd/io/SimpleSchedulerObjectDispatch.h"
#include <boost/algorithm/string/predicate.hpp>
#include "include/ceph_assert.h"

#include "include/cpp-bplustree/BpTree.hpp"

#include <fstream>
#include <iostream>
#include <ctime>
using namespace std;

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::snaptree::OpenRequest: "

namespace librbd {
    namespace snaptree {

        using util::create_context_callback;
        using util::create_rados_callback;

        template <typename I>
        OpenRequest<I>::OpenRequest(I *image_ctx, uint64_t flags,
                                    Context *on_finish)
                : m_image_ctx(image_ctx),
                  m_skip_open_parent_image(flags & OPEN_FLAG_SKIP_OPEN_PARENT),
                  m_on_finish(on_finish), m_error_result(0) {
            if ((flags & OPEN_FLAG_OLD_FORMAT) != 0) {
                m_image_ctx->old_format = true;
            }
            if ((flags & OPEN_FLAG_IGNORE_MIGRATING) != 0) {
                m_image_ctx->ignore_migrating = true;
            }
        }

        template <typename I>
        void OpenRequest<I>::send() {
            if (m_image_ctx->old_format) {
                send_v1_detect_header();
            } else {
                ofstream ofile;
                ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
                if(!ofile.is_open()){
                    cout<<"open file error!";
                }
                time_t now = time(0);
                char* dt = ctime(&now);
                ofile<<dt<<"come to librbd::snaptree::OpenRequest.cc::send()\n";
                ofile.close();
                send_v2_detect_header();
            }
        }

        template <typename I>
        void OpenRequest<I>::send_v1_detect_header() {
            librados::ObjectReadOperation op;
            op.stat(NULL, NULL, NULL);

            using klass = OpenRequest<I>;
            librados::AioCompletion *comp =
                    create_rados_callback<klass, &klass::handle_v1_detect_header>(this);
            m_out_bl.clear();
            m_image_ctx->md_ctx.aio_operate(util::old_header_name(m_image_ctx->name),
                                            comp, &op, &m_out_bl);
            comp->release();
        }

        template <typename I>
        Context *OpenRequest<I>::handle_v1_detect_header(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            if (*result < 0) {
                if (*result != -ENOENT) {
                    lderr(cct) << "failed to stat snaptree header: " << cpp_strerror(*result)
                               << dendl;
                }
                send_close_image(*result);
            } else {
                ldout(cct, 1) << "RBD image format 1 is deprecated. "
                              << "Please copy this image to image format 2." << dendl;

                m_image_ctx->old_format = true;
                m_image_ctx->header_oid = util::old_header_name(m_image_ctx->name);
                m_image_ctx->apply_metadata({}, true);

                send_refresh();
            }
            return nullptr;
        }

        template <typename I>
        void OpenRequest<I>::send_v2_detect_header() {
            if (m_image_ctx->id.empty()) {
                CephContext *cct = m_image_ctx->cct;
                ldout(cct, 10) << this << " " << __func__ << dendl;

                librados::ObjectReadOperation op;
                op.stat(NULL, NULL, NULL);
                ofstream ofile;
                ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
                if(!ofile.is_open()){
                    cout<<"open file error!";
                }
                time_t now = time(0);
                char* dt = ctime(&now);
                ofile<<dt<<"come to librbd::snaptree::OpenRequest.cc::send_v2_detect_header()\n";
                ofile.close();
                using klass = OpenRequest<I>;
                librados::AioCompletion *comp =
                        create_rados_callback<klass, &klass::handle_v2_detect_header>(this);
                m_out_bl.clear();
                m_image_ctx->md_ctx.aio_operate(util::id_obj_name(m_image_ctx->name),
                                                comp, &op, &m_out_bl);
                comp->release();
            } else {
                send_v2_get_name();
            }
        }

        template <typename I>
        Context *OpenRequest<I>::handle_v2_detect_header(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            if (*result == -ENOENT) {
                send_v1_detect_header();
            } else if (*result < 0) {
                lderr(cct) << "failed to stat v2 snaptree header: " << cpp_strerror(*result)
                           << dendl;
                send_close_image(*result);
            } else {
                m_image_ctx->old_format = false;
                send_v2_get_id();
            }
            return nullptr;
        }

        template <typename I>
        void OpenRequest<I>::send_v2_get_id() {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            librados::ObjectReadOperation op;
            cls_client::get_id_start(&op);
            ofstream ofile;
            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
            if(!ofile.is_open()){
                cout<<"open file error!";
            }
            time_t now = time(0);
            char* dt = ctime(&now);
            ofile<<dt<<"come to librbd::snaptree::OpenRequest.cc::send_v2_get_id()\n";
            ofile.close();
            using klass = OpenRequest<I>;
            librados::AioCompletion *comp =
                    create_rados_callback<klass, &klass::handle_v2_get_id>(this);
            m_out_bl.clear();
            m_image_ctx->md_ctx.aio_operate(util::id_obj_name(m_image_ctx->name),
                                            comp, &op, &m_out_bl);
            comp->release();
        }

        template <typename I>
        Context *OpenRequest<I>::handle_v2_get_id(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            if (*result == 0) {
                auto it = m_out_bl.cbegin();
                *result = cls_client::get_id_finish(&it, &m_image_ctx->id);
            }
            if (*result < 0) {
                lderr(cct) << "failed to retrieve snaptree id: " << cpp_strerror(*result)
                           << dendl;
                send_close_image(*result);
            } else {
                send_v2_get_initial_metadata();
            }
            return nullptr;
        }

        template <typename I>
        void OpenRequest<I>::send_v2_get_name() {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            librados::ObjectReadOperation op;
            cls_client::dir_get_name_start(&op, m_image_ctx->id);

            using klass = OpenRequest<I>;
            librados::AioCompletion *comp = create_rados_callback<
                    klass, &klass::handle_v2_get_name>(this);
            m_out_bl.clear();
            m_image_ctx->md_ctx.aio_operate(RBD_DIRECTORY, comp, &op, &m_out_bl);
            comp->release();
        }

        template <typename I>
        Context *OpenRequest<I>::handle_v2_get_name(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            if (*result == 0) {
                auto it = m_out_bl.cbegin();
                *result = cls_client::dir_get_name_finish(&it, &m_image_ctx->name);
            }
            if (*result < 0 && *result != -ENOENT) {
                lderr(cct) << "failed to retrieve name: "
                           << cpp_strerror(*result) << dendl;
                send_close_image(*result);
            } else if (*result == -ENOENT) {
                // image does not exist in directory, look in the trash bin
                ldout(cct, 10) << "image id " << m_image_ctx->id << " does not exist in "
                               << "rbd directory, searching in rbd trash..." << dendl;
                send_v2_get_name_from_trash();
            } else {
                ofstream ofile;
                ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
                if(!ofile.is_open()){
                    cout<<"open file error!";
                }
                time_t now = time(0);
                char* dt = ctime(&now);
                ofile<<dt<<"come to librbd::snaptree::OpenRequest.cc::handle_v2_get_name()\n";
                ofile.close();
                send_v2_get_initial_metadata();
            }
            return nullptr;
        }

        template <typename I>
        void OpenRequest<I>::send_v2_get_name_from_trash() {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            librados::ObjectReadOperation op;
            cls_client::trash_get_start(&op, m_image_ctx->id);

            using klass = OpenRequest<I>;
            librados::AioCompletion *comp = create_rados_callback<
                    klass, &klass::handle_v2_get_name_from_trash>(this);
            m_out_bl.clear();
            m_image_ctx->md_ctx.aio_operate(RBD_TRASH, comp, &op, &m_out_bl);
            comp->release();
        }

        template <typename I>
        Context *OpenRequest<I>::handle_v2_get_name_from_trash(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            cls::rbd::TrashImageSpec trash_spec;
            if (*result == 0) {
                auto it = m_out_bl.cbegin();
                *result = cls_client::trash_get_finish(&it, &trash_spec);
                m_image_ctx->name = trash_spec.name;
            }
            if (*result < 0) {
                if (*result == -EOPNOTSUPP) {
                    *result = -ENOENT;
                }
                if (*result == -ENOENT) {
                    ldout(cct, 5) << "failed to retrieve name for image id "
                                  << m_image_ctx->id << dendl;
                } else {
                    lderr(cct) << "failed to retrieve name from trash: "
                               << cpp_strerror(*result) << dendl;
                }
                send_close_image(*result);
            } else {
                send_v2_get_initial_metadata();
            }

            return nullptr;
        }

        template <typename I>
        void OpenRequest<I>::send_v2_get_initial_metadata() {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            m_image_ctx->old_format = false;
            m_image_ctx->header_oid = util::header_name(m_image_ctx->id);

            librados::ObjectReadOperation op;
            cls_client::get_size_start(&op, CEPH_NOSNAP);
            cls_client::get_object_prefix_start(&op);
            cls_client::get_features_start(&op, true);

            using klass = OpenRequest<I>;
            librados::AioCompletion *comp = create_rados_callback<
                    klass, &klass::handle_v2_get_initial_metadata>(this);
            m_out_bl.clear();
            m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                            &m_out_bl);

            ofstream ofile;
            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
            if(!ofile.is_open()){
                cout<<"open file error!";
            }
            time_t now = time(0);
            char* dt = ctime(&now);
            ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::send_v2_get_initial_metadata()\n";
            ofile.close();
            comp->release();
        }

        template <typename I>
        Context *OpenRequest<I>::handle_v2_get_initial_metadata(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            auto it = m_out_bl.cbegin();
            if (*result >= 0) {
                uint64_t size;
                *result = cls_client::get_size_finish(&it, &size, &m_image_ctx->order);
                m_size=size;
            }

            if (*result >= 0) {
                *result = cls_client::get_object_prefix_finish(&it,
                                                               &m_image_ctx->object_prefix);
            }

            if (*result >= 0) {
                uint64_t incompatible_features;
                *result = cls_client::get_features_finish(&it, &m_image_ctx->features,
                                                          &incompatible_features);
            }

            if (*result < 0) {
                lderr(cct) << "failed to retrieve initial metadata: "
                           << cpp_strerror(*result) << dendl;
                send_close_image(*result);
                return nullptr;
            }

            if (m_image_ctx->test_features(RBD_FEATURE_STRIPINGV2)) {
                ofstream ofile;
                ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
                if(!ofile.is_open()){
                    cout<<"open file error!";
                }
                time_t now = time(0);
                char* dt = ctime(&now);
                ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::handle_v2_get_initial_metadata()|| test_features\n";
                ofile.close();
                send_v2_get_stripe_unit_count();
            } else {
                ofstream ofile;
                ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
                if(!ofile.is_open()){
                    cout<<"open file error!";
                }
                time_t now = time(0);
                char* dt = ctime(&now);
                ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::handle_v2_get_initial_metadata()|| test_features elseeeee\n";
                ofile.close();
                send_v2_get_create_timestamp();
            }

            return nullptr;
        }

        template <typename I>
        void OpenRequest<I>::send_v2_get_stripe_unit_count() {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;
//            ofstream ofile;
//            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
//            if(!ofile.is_open()){
//                cout<<"open file error!";
//            }
//            time_t now = time(0);
//            char* dt = ctime(&now);
//            ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::send_v2_get_stripe_unit_count()\n";
//            ofile.close();
            librados::ObjectReadOperation op;
            cls_client::get_stripe_unit_count_start(&op);

            using klass = OpenRequest<I>;
            librados::AioCompletion *comp = create_rados_callback<
                    klass, &klass::handle_v2_get_stripe_unit_count>(this);
            m_out_bl.clear();
            m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                            &m_out_bl);
            comp->release();
        }

        template <typename I>
        Context *OpenRequest<I>::handle_v2_get_stripe_unit_count(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            if (*result == 0) {
                auto it = m_out_bl.cbegin();
                *result = cls_client::get_stripe_unit_count_finish(
                        &it, &m_image_ctx->stripe_unit, &m_image_ctx->stripe_count);
            }
//            ofstream ofile;
//            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
//            if(!ofile.is_open()){
//                cout<<"open file error!";
//            }
//            time_t now = time(0);
//            char* dt = ctime(&now);
//            ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::handle_v2_get_stripe_unit_count()||stripe_unit:"<<m_image_ctx->stripe_unit
//                 <<" stripe_count:"<<m_image_ctx->stripe_count
//                 <<" size:"<<m_image_ctx->size
//                 <<"\n";
//            ofile.close();
            if (*result == -ENOEXEC || *result == -EINVAL) {
                *result = 0;
            }

            if (*result < 0) {
                lderr(cct) << "failed to read striping metadata: " << cpp_strerror(*result)
                           << dendl;
                send_close_image(*result);
                return nullptr;
            }

            send_v2_get_create_timestamp();
            return nullptr;
        }

        template <typename I>
        void OpenRequest<I>::send_v2_get_create_timestamp() {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            librados::ObjectReadOperation op;
            cls_client::get_create_timestamp_start(&op);

            using klass = OpenRequest<I>;
            librados::AioCompletion *comp = create_rados_callback<
                    klass, &klass::handle_v2_get_create_timestamp>(this);
            m_out_bl.clear();
            m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                            &m_out_bl);
            comp->release();
        }

        template <typename I>
        Context *OpenRequest<I>::handle_v2_get_create_timestamp(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

            if (*result == 0) {
                auto it = m_out_bl.cbegin();
                *result = cls_client::get_create_timestamp_finish(&it,
                                                                  &m_image_ctx->create_timestamp);
            }
            if (*result < 0 && *result != -EOPNOTSUPP) {
                lderr(cct) << "failed to retrieve create_timestamp: "
                           << cpp_strerror(*result)
                           << dendl;
                send_close_image(*result);
                return nullptr;
            }
//            ofstream ofile;
//            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
//            if(!ofile.is_open()){
//                cout<<"open file error!";
//            }
//            time_t now = time(0);
//            char* dt = ctime(&now);
//            ofile<<dt<< " come to librbd::snaptree::OpenRequest.cc::handle_v2_get_create_timestamp()\n";
//            ofile.close();
            send_v2_get_access_modify_timestamp();
            return nullptr;
        }

        template <typename I>
        void OpenRequest<I>::send_v2_get_access_modify_timestamp() {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            librados::ObjectReadOperation op;
            cls_client::get_access_timestamp_start(&op);
            cls_client::get_modify_timestamp_start(&op);
            //TODO: merge w/ create timestamp query after luminous EOLed
//            ofstream ofile;
//            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
//            if(!ofile.is_open()){
//                cout<<"open file error!";
//            }
//            time_t now = time(0);
//            char* dt = ctime(&now);
//            ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::send_v2_get_access_modify_timestamp()"<<"\n";
//            ofile.close();
            using klass = OpenRequest<I>;
            librados::AioCompletion *comp = create_rados_callback<
                    klass, &klass::handle_v2_get_access_modify_timestamp>(this);
            m_out_bl.clear();
            m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                            &m_out_bl);
            comp->release();
        }

        template <typename I>
        Context *OpenRequest<I>::handle_v2_get_access_modify_timestamp(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

            if (*result == 0) {
                auto it = m_out_bl.cbegin();
                *result = cls_client::get_access_timestamp_finish(&it,
                                                                  &m_image_ctx->access_timestamp);
                if (*result == 0)
                    *result = cls_client::get_modify_timestamp_finish(&it,
                                                                      &m_image_ctx->modify_timestamp);
            }
            if (*result < 0 && *result != -EOPNOTSUPP) {
                lderr(cct) << "failed to retrieve access/modify_timestamp: "
                           << cpp_strerror(*result)
                           << dendl;
                send_close_image(*result);
                return nullptr;
            }
//            ofstream ofile;
//            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
//            if(!ofile.is_open()){
//                cout<<"open file error!";
//            }
//            time_t now = time(0);
//            char* dt = ctime(&now);
//            ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::handle_v2_get_access_modify_timestamp()"<<"\n";
//            ofile.close();
            send_v2_get_data_pool();
            return nullptr;
        }

        template <typename I>
        void OpenRequest<I>::send_v2_get_data_pool() {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            librados::ObjectReadOperation op;
            cls_client::get_data_pool_start(&op);

            using klass = OpenRequest<I>;
            librados::AioCompletion *comp = create_rados_callback<
                    klass, &klass::handle_v2_get_data_pool>(this);
            m_out_bl.clear();
            m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                            &m_out_bl);
            comp->release();
        }

        template <typename I>
        Context *OpenRequest<I>::handle_v2_get_data_pool(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

            int64_t data_pool_id = -1;
            if (*result == 0) {
                auto it = m_out_bl.cbegin();
                *result = cls_client::get_data_pool_finish(&it, &data_pool_id);
            } else if (*result == -EOPNOTSUPP) {
                *result = 0;
            }

            if (*result < 0) {
                lderr(cct) << "failed to read data pool: " << cpp_strerror(*result)
                           << dendl;
                send_close_image(*result);
                return nullptr;
            }

            if (data_pool_id != -1) {
                *result = util::create_ioctx(m_image_ctx->md_ctx, "data pool", data_pool_id,
                                             {}, &m_image_ctx->data_ctx);
                if (*result < 0) {
                    if (*result != -ENOENT) {
                        send_close_image(*result);
                        return nullptr;
                    }
                    m_image_ctx->data_ctx.close();
                } else {
                    m_image_ctx->data_ctx.set_namespace(m_image_ctx->md_ctx.get_namespace());
                }
            } else {
                data_pool_id = m_image_ctx->md_ctx.get_id();
            }

            m_image_ctx->init_layout(data_pool_id);
            ofstream ofile;
            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
            if(!ofile.is_open()){
                cout<<"open file error!";
            }
            ofile<<" come to librbd::snaptree::OpenRequest.cc::handle_v2_get_object_map_snapid()||tree_init_once:"<<ImageCtx::tree_init_once<<"\n";
            if(!ImageCtx::tree_init_once){
                ofile<<" come to librbd::snaptree::OpenRequest.cc::handle_v2_get_object_map_snapid()||built:"<<m_image_ctx->imagetree_is_built<<"\n";
                send_v2_get_image_object_map_snapid();
                m_image_ctx->imagetree_is_built=1;
                m_image_ctx->set_tree_init_once();
                ofile<<" come to librbd::snaptree::OpenRequest.cc::handle_v2_get_object_map_snapid()||tree_init_once in :"<<ImageCtx::tree_init_once<<"\n";
                ofile<<" come to librbd::snaptree::OpenRequest.cc::handle_v2_get_object_map_snapid()||built:"<<m_image_ctx->imagetree_is_built<<"\n";
                return nullptr;
            }

            send_refresh();
            return nullptr;
        }

        //处理head image.将数据块中的imageid读取出来构建成bptree
        template <typename I>
        void OpenRequest<I>::send_v2_get_image_object_map_snapid() {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;
//    m_image_ctx->snapmap_oid = util::id_obj_map_snapid_name(m_image_ctx->id);
            ofstream ofile;
            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
            if(!ofile.is_open()){
                cout<<"open file error!";
            }
            time_t now = time(0);
            char* dt = ctime(&now);
            ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::send_v2_get_image_object_map_snapid()||internal_\n";
            librados::ObjectReadOperation op;
            m_layout.stripe_unit=m_image_ctx->stripe_unit;
            m_layout.stripe_count=m_image_ctx->stripe_count;
            m_layout.object_size=m_layout.stripe_unit;

            ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::send_v2_get_image_object_map_snapid()||stripe_unit:"<<m_layout.stripe_unit
                 <<" stripe_count:"<<m_layout.stripe_count
                 <<" size:"<<m_size
                 <<" period():"<<m_layout.get_period()<<"\n";

            m_count=Striper::get_num_objects(m_layout,m_size);
            m_image_ctx->object_count=m_count;
            ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::send_v2_get_image_object_map_snapid()||get_num_objects:"<<m_count<<"\n";
            ofile.close();

            cls_client::get_object_map_imageid_start(&op,m_count,m_image_ctx->id);
            using klass = OpenRequest<I>;
            librados::AioCompletion *comp = create_rados_callback<
                    klass, &klass::handle_v2_get_image_object_map_snapid>(this);
            m_out_bl.clear();
            object_map_imageid_bl.clear();
            m_image_ctx->md_ctx.aio_operate(util::id_obj_map_snapid_name(m_image_ctx->id), comp, &op,
                                            &m_out_bl);
            comp->release();

        }
        template <typename I>
        Context *OpenRequest<I>::handle_v2_get_image_object_map_snapid(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;
            if(*result==0){
                auto it=m_out_bl.cbegin();
                *result=cls_client::get_object_map_imageid_finish(&it,&m_image_ctx->object_map_imageid_bpl,m_count);
                string object_image_id;
                auto iter=m_image_ctx->object_map_imageid_bpl.cbegin();
                for(uint64_t i=0;i<m_count;i++){
                    try {
                        decode(object_image_id,iter);
                        m_image_ctx->head_image_bptree.insert(i,object_image_id);
                    } catch (const buffer::error &err) {
                        return nullptr;
                    }
                }
            }
            m_image_ctx->head_image_bptree.printKeys();
            m_image_ctx->head_image_bptree.printValues();
            string get_image_id;
            try {
                auto iter=m_image_ctx->object_map_imageid_bpl.cbegin();
                decode(get_image_id,iter);
            } catch (const buffer::error &err) {
                return reinterpret_cast<Context *>(-EINVAL);
            }
            //用完之后将bufferlist清空
//            m_image_ctx->object_map_snapid_bpl.clear();
            ofstream ofile;
            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
            if(!ofile.is_open()){
                cout<<"open file error!";
            }
            time_t now = time(0);
            char* dt = ctime(&now);
            ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::handle_v2_get_object_map_snapid()|| snaptree id:"<<get_image_id<<"\n";
            ofile.close();

            if (*result < 0) {
                lderr(cct) << "failed to get object map snapid : " << cpp_strerror(*result)
                           << dendl;
                send_close_image(*result);
                return nullptr;
            }
//            send_refresh();
            send_v2_get_snapshots();
            return nullptr;
//    return finalize(*result);
        }
        template <typename I>
        void OpenRequest<I>::send_v2_get_snapshots(){
             CephContext *cct = m_image_ctx->cct;
             ldout(cct, 10) << this << " " << __func__ << dendl;

              librados::ObjectReadOperation op;
              cls_client::get_snapcontext_start(&op);

             using klass = OpenRequest<I>;
             librados::AioCompletion *comp = create_rados_callback<
                 klass, &klass::handle_v2_get_snapshots>(this);
             m_out_bl.clear();
             int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                           &m_out_bl);
             ceph_assert(r == 0);
             comp->release();
        }
        template <typename I>
        Context *OpenRequest<I>::handle_v2_get_snapshots(int *result){
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << ": "
                           << "r=" << *result << dendl;

            auto it = m_out_bl.cbegin();
            if (*result >= 0) {
                *result = cls_client::get_snapcontext_finish(&it, &m_snapc);
            }
            if (*result < 0) {
                lderr(cct) << "failed to retrieve mutable metadata: "
                           << cpp_strerror(*result) << dendl;
                return m_on_finish;
            }
            if (!m_snapc.is_valid()) {
                lderr(cct) << "image snap context is invalid!" << dendl;
                *result = -EIO;
                return m_on_finish;
            }
            send_v2_get_snap_object_map_snapid();
            return nullptr;
        }
        //设置每个映射的数据块的名称
        template <typename I>
        std::string OpenRequest<I>::object_map_name(const std::string &image_id,
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
        void OpenRequest<I>::send_v2_get_snap_object_map_snapid(){
            CephContext *cct=m_image_ctx->cct;
            ldout(cct,10)<<this<<" "<<__func__<<dendl;
            ofstream ofile;
            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
            if(!ofile.is_open()){
                cout<<"open file error!";
            }
            ofile<<"come to librbd::image::RefreshRequest.cc::send_v2_get_snap_object_map_snapid()\n";
            if (m_snapc.snaps.empty()) {
                ofile<<"come to librbd::image::RefreshRequest.cc::send_v2_get_snap_object_map_snapid()|| snap is empty\n";
                return;
            }
            ofile<<"come to librbd::image::RefreshRequest.cc::send_v2_get_snap_object_map_snapid()||snaps.size:"<<m_snapc.snaps.size()<<"\n";
            for(unsigned int i=0;i<m_snapc.snaps.size();++i){
                ofile<<"snap_id:"<<m_snapc.snaps[i]<<"\n";
            }
            ofile<<"come to librbd::image::RefreshRequest.cc::send_v2_get_snap_object_map_snapid()|| after iterate the snaps\n";


            for(auto snap_id:m_snapc.snaps){
                ofile<<"come to librbd::image::RefreshRequest.cc::send_v2_get_snap_object_map_snapid()|| snap_id:"<<snap_id<<"\n";
                librados::ObjectReadOperation op;
                //cls_client就是让op做什么事--read
                int obj_count=m_image_ctx->get_object_count(snap_id);
                cls_client::get_object_map_snapid_start(&op,obj_count,snap_id);
                std::string oid(OpenRequest<I>::object_map_name(m_image_ctx->id, snap_id));
                using klass=OpenRequest<I>;
                librados::AioCompletion *comp = create_rados_callback<
                        klass, &klass::handle_v2_get_snap_object_map_snapid>(this);
                m_out_bl.clear();
                int r = m_image_ctx->md_ctx.aio_operate(oid, comp, &op,&m_out_bl);
                ceph_assert(r == 0);
                comp->release();
            }
//        snapid_t snap_id=m_snapc.snaps[0];
//        ofile<<"come to librbd::image::RefreshRequest.cc::send_v2_get_snap_object_map_snapid()|| snap_id:"<<snap_id<<"\n";
//        librados::ObjectReadOperation op;
//        //cls_client就是让op做什么事--read
//        cls_client::get_object_map_snapid_start(&op,m_image_ctx.object_count,snap_id);
//        std::string oid(RefreshRequest<I>::object_map_name(m_image_ctx.id, snap_id));
//        using klass=RefreshRequest<I>;
//        librados::AioCompletion *comp = create_rados_callback<
//                klass, &klass::handle_v2_get_snap_object_map_snapid>(this);
//        m_out_bl.clear();
//        int r = m_image_ctx.md_ctx.aio_operate(oid, comp, &op,&m_out_bl);
//        ceph_assert(r == 0);
//        comp->release();
            ofile<<"come to librbd::image::RefreshRequest.cc::send_v2_get_snap_object_map_snapid()||finished\n";
            ofile.close();

        }
        template <typename I>
        Context* OpenRequest<I>::handle_v2_get_snap_object_map_snapid(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 20) << __func__ << ": r=" << *result << dendl;

            ofstream ofile;
            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
            if(!ofile.is_open()){
                cout<<"open file error!";
            }
            ofile<<"come to librbd::image::RefreshRequest.cc::handle_v2_get_snap_object_map_snapid()\n";
            ofile<<"come to librbd::image::RefreshRequest.cc::handle_v2_get_snap_object_map_snapid()|| result:"<<*result<<"\n";
            if (*result == 0) {
                auto it = m_out_bl.cbegin();
                //将下面的代码改成数组访问模式，不需要cls_client
                bufferlist *temp_bpl;
                *result = cls_client::get_object_map_snapid_finish(&it, temp_bpl,
                                                                   m_image_ctx->object_count);
                auto iter = temp_bpl->cbegin();
                snapid_t prev_snap_id;//当前快照的前一个快照id
                snapid_t current_snap_id;//当前快照自身的快照id
                snapid_t object_snap_id;//数据块快照id
                try {
                    decode(prev_snap_id, iter);
                    decode(current_snap_id, iter);
                } catch (const buffer::error &err) {
                    return nullptr;
                }
                //这里需要获取到两棵树，然后根据前一个树来构建下一棵树（有一个bug，如果是第一个快照，他的前面就是head image，没有snap_prev
                // ,这里需要添加判断(在set的时候需要分开讨论，这里暂定如果是第一个快照，prev则是同样是自己)）
                ofile<<"come to librbd::image::RefreshRequest.cc::handle_v2_get_snap_object_map_snapid()||prev_snap_id:"<<prev_snap_id<<"\n";
                ofile<<"come to librbd::image::RefreshRequest.cc::handle_v2_get_snap_object_map_snapid()||current_snap_id:"<<current_snap_id<<"\n";
                //相等则为第一个快照,prev则是head image
                if (prev_snap_id == current_snap_id) {
                    Node *tree_head = m_image_ctx->head_image_bptree.getRootNode();
                    BpTree snap_bptree((In_Node &) (*tree_head));
                    std::ostringstream o;
                    for (int i = 0; i < m_image_ctx->object_count; i++) {
                        try {
                            decode(object_snap_id, iter);
                            if (object_snap_id == 0) {
                                o << object_snap_id;
                                snap_bptree.insertSnapValue(i, o.str(), (In_Node *) (tree_head));
                                o.str("");
                            }
                        } catch (const buffer::error &err) {
                            return nullptr;
                        }
                    }
                    ofile<<"come to librbd::image::RefreshRequest.cc::handle_v2_get_snap_object_map_snapid()|| first snap and print\n";
                    snap_bptree.printValues();
                    m_image_ctx->snap_tree_set.insert(pair<snapid_t, BpTree>(current_snap_id, snap_bptree));
                } else {//不相等则不是第一个快照,snap_prev是快照,需要从map中获取到bptree然后构建当前快照的bptree,最后添加到map.
                    for (auto it = m_image_ctx->snap_tree_set.begin(); it != m_image_ctx->snap_tree_set.end(); it++) {
                        if (it->first == prev_snap_id) {
                            Node *tree_head = it->second.getRootNode();
                            BpTree snap_bptree((In_Node &) (*tree_head));
                            std::ostringstream o;
                            for (int i = 0; i < m_image_ctx->object_count; i++) {
                                try {
                                    decode(object_snap_id, iter);
                                    if (object_snap_id != 0) {
                                        o << object_snap_id;
                                        snap_bptree.insertSnapValue(i, o.str(), (In_Node *) (tree_head));
                                        o.str("");
                                    }
                                } catch (const buffer::error &err) {
                                    return nullptr;
                                }
                            }
                            ofile<<"come to librbd::image::RefreshRequest.cc::handle_v2_get_snap_object_map_snapid()|| not first snap and print\n";
                            snap_bptree.printValues();
                            m_image_ctx->snap_tree_set.insert(pair<snapid_t, BpTree>(current_snap_id, snap_bptree));
                        }

                    }

                }
                //判断是否是最后一个快照，如果是则完成refresh后续步骤
                if(m_snapc.snaps.back()==current_snap_id){
                    send_refresh();
                    return nullptr;
                }
            }
            ofile.close();
            if (*result < 0) {
                lderr(cct) << "get snap object map snapid failed: " << cpp_strerror(*result)
                           << dendl;
            }
            return nullptr;
        }

        template <typename I>
        void OpenRequest<I>::send_refresh() {
            ofstream ofile;
            ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
            if(!ofile.is_open()){
                cout<<"open file error!";
            }
            ofile<<" come to librbd::snaptree::OpenRequest.cc::send_refresh()|| before init "<<m_image_ctx->tree_init_once<<"\n";
            m_image_ctx->init();
            ofile<<" come to librbd::snaptree::OpenRequest.cc::send_refresh()|| after init "<<m_image_ctx->tree_init_once<<"\n";
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            m_image_ctx->config_watcher = ConfigWatcher<I>::create(*m_image_ctx);
            m_image_ctx->config_watcher->init();

            time_t now = time(0);
            char* dt = ctime(&now);
            ofile<<dt<<" come to librbd::snaptree::OpenRequest.cc::send_refresh()\n";
            ofile.close();
            using klass = OpenRequest<I>;
            image::RefreshRequest<I> *req = image::RefreshRequest<I>::create(
                    *m_image_ctx, false, m_skip_open_parent_image,
                    create_context_callback<klass, &klass::handle_refresh>(this));
            req->send();
        }

        template <typename I>
        Context *OpenRequest<I>::handle_refresh(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            if (*result < 0) {
                lderr(cct) << "failed to refresh image: " << cpp_strerror(*result)
                           << dendl;
                send_close_image(*result);
                return nullptr;
            }
            return send_parent_cache(result);
        }

        template <typename I>
        Context* OpenRequest<I>::send_parent_cache(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            bool parent_cache_enabled = m_image_ctx->config.template get_val<bool>(
                    "rbd_parent_cache_enabled");

            if (m_image_ctx->child == nullptr || !parent_cache_enabled) {
                return send_init_cache(result);
            }

            auto parent_cache = cache::ParentCacheObjectDispatch<I>::create(m_image_ctx);
            using klass = OpenRequest<I>;
            Context *ctx = create_context_callback<
                    klass, &klass::handle_parent_cache>(this);

            parent_cache->init(ctx);
            return nullptr;
        }

        template <typename I>
        Context* OpenRequest<I>::handle_parent_cache(int* result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            if (*result < 0) {
                lderr(cct) << "failed to parent cache " << dendl;
                send_close_image(*result);
                return nullptr;
            }

            return send_init_cache(result);
        }

        template <typename I>
        Context *OpenRequest<I>::send_init_cache(int *result) {
            // cache is disabled or parent image context
            if (!m_image_ctx->cache || m_image_ctx->child != nullptr) {
                return send_register_watch(result);
            }

            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            size_t max_dirty = m_image_ctx->config.template get_val<Option::size_t>(
                    "rbd_cache_max_dirty");
            auto writethrough_until_flush = m_image_ctx->config.template get_val<bool>(
                    "rbd_cache_writethrough_until_flush");
            auto cache_policy = m_image_ctx->config.template get_val<std::string>(
                    "rbd_cache_policy");
            if (cache_policy == "writearound") {
                auto cache = cache::WriteAroundObjectDispatch<I>::create(
                        m_image_ctx, max_dirty, writethrough_until_flush);
                cache->init();
            } else if (cache_policy == "writethrough" || cache_policy == "writeback") {
                if (cache_policy == "writethrough") {
                    max_dirty = 0;
                }

                auto cache = cache::ObjectCacherObjectDispatch<I>::create(
                        m_image_ctx, max_dirty, writethrough_until_flush);
                cache->init();

                // readahead requires the object cacher cache
                m_image_ctx->readahead.set_trigger_requests(
                        m_image_ctx->config.template get_val<uint64_t>("rbd_readahead_trigger_requests"));
                m_image_ctx->readahead.set_max_readahead_size(
                        m_image_ctx->config.template get_val<Option::size_t>("rbd_readahead_max_bytes"));
            }
            return send_register_watch(result);
        }

        template <typename I>
        Context *OpenRequest<I>::send_register_watch(int *result) {
            if ((m_image_ctx->read_only_flags & IMAGE_READ_ONLY_FLAG_USER) != 0U) {
                return send_set_snap(result);
            }

            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            using klass = OpenRequest<I>;
            Context *ctx = create_context_callback<
                    klass, &klass::handle_register_watch>(this);
            m_image_ctx->register_watch(ctx);
            return nullptr;
        }

        template <typename I>
        Context *OpenRequest<I>::handle_register_watch(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

            if (*result == -EPERM) {
                ldout(cct, 5) << "user does not have write permission" << dendl;
                send_close_image(*result);
                return nullptr;
            } else if (*result < 0) {
                lderr(cct) << "failed to register watch: " << cpp_strerror(*result)
                           << dendl;
                send_close_image(*result);
                return nullptr;
            }

            return send_set_snap(result);
        }

        template <typename I>
        Context *OpenRequest<I>::send_set_snap(int *result) {
            if (m_image_ctx->snap_name.empty() &&
                m_image_ctx->open_snap_id == CEPH_NOSNAP) {
                *result = 0;
                return finalize(*result);
            }

            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            uint64_t snap_id = CEPH_NOSNAP;
            std::swap(m_image_ctx->open_snap_id, snap_id);
            if (snap_id == CEPH_NOSNAP) {
                std::shared_lock image_locker{m_image_ctx->image_lock};
                snap_id = m_image_ctx->get_snap_id(m_image_ctx->snap_namespace,
                                                   m_image_ctx->snap_name);
            }
            if (snap_id == CEPH_NOSNAP) {
                lderr(cct) << "failed to find snapshot " << m_image_ctx->snap_name << dendl;
                send_close_image(-ENOENT);
                return nullptr;
            }

            using klass = OpenRequest<I>;
            image::SetSnapRequest<I> *req = image::SetSnapRequest<I>::create(
                    *m_image_ctx, snap_id,
                    create_context_callback<klass, &klass::handle_set_snap>(this));
            req->send();
            return nullptr;
        }

        template <typename I>
        Context *OpenRequest<I>::handle_set_snap(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            if (*result < 0) {
                lderr(cct) << "failed to set image snapshot: " << cpp_strerror(*result)
                           << dendl;
                send_close_image(*result);
                return nullptr;
            }
//            send_v2_get_image_object_map_snapid();
            return nullptr;
//  return finalize(*result);
        }

        template <typename I>
        Context *OpenRequest<I>::finalize(int r) {
            if (r == 0) {
                auto io_scheduler_cfg =
                        m_image_ctx->config.template get_val<std::string>("rbd_io_scheduler");

                if (io_scheduler_cfg == "simple" && !m_image_ctx->read_only) {
                    auto io_scheduler =
                            io::SimpleSchedulerObjectDispatch<I>::create(m_image_ctx);
                    io_scheduler->init();
                }
            }

            return m_on_finish;
        }

        template <typename I>
        void OpenRequest<I>::send_close_image(int error_result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << this << " " << __func__ << dendl;

            m_error_result = error_result;

            using klass = OpenRequest<I>;
            Context *ctx = create_context_callback<klass, &klass::handle_close_image>(
                    this);
            image::CloseRequest<I> *req = image::CloseRequest<I>::create(m_image_ctx, ctx);
            req->send();
        }

        template <typename I>
        Context *OpenRequest<I>::handle_close_image(int *result) {
            CephContext *cct = m_image_ctx->cct;
            ldout(cct, 10) << __func__ << ": r=" << *result << dendl;

            if (*result < 0) {
                lderr(cct) << "failed to close image: " << cpp_strerror(*result) << dendl;
            }
            if (m_error_result < 0) {
                *result = m_error_result;
            }
            return m_on_finish;
        }

    } // namespace snaptree
} // namespace librbd



template class librbd::snaptree::OpenRequest<librbd::ImageCtx>;