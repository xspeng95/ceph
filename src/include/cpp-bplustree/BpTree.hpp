//
//  BpTree.h
//  BpTree
//
//  Created by May Wu on 16/6/30.
//  Copyright © 2016 May Wu All rights reserved.
//

#ifndef BpTree_h
#define BpTree_h

#include <iostream>
#include <fstream>
#include <cmath>
#include <vector>
#include "Node.hpp"


class BpTree
{
private:

    int n;
    Node* head;
public:

    BpTree(int nptr)
    {
        this->n = nptr;
        this->head = new LeafNode();
    }
    BpTree(const BpTree&){

    }
    BpTree(In_Node& node)
    {

        head=new In_Node(node);

        head->isExsit=true;
        cout<<"construct ..."<<"\n";
    }
    void insert(int key, string value)
    {
        Node* t=this->head;
//    cout<<" isLeaf:"<<t->isLeaf<<endl;
        while (!t->isLeaf){ //find the proper location
            int flag = 0;
            for (size_t i=0;i<t->Keys.size();++i){
                if(t->Keys[i]>key){
                    t = ((In_Node*)t)->Children[i];
                    flag = 1;
                    break;
                }
            }
            if(!flag){
                t = ((In_Node*)t)->Children[t->Keys.size()];
            }
        }
        if(t->Keys.size()==0||key>t->Keys.back()){
            t->Keys.push_back(key);
            ((LeafNode*)t)->Value.push_back(value);
        }
        else{
            for(size_t i=0;i<t->Keys.size();++i){ //insert into the leaf node
                if(t->Keys[i]==key){
                    cout<<"You cannot insert duplicate keys!"<<"\n";
                    return;
                }
                else if(t->Keys[i]>key){
                    t->Keys.insert(t->Keys.begin() + i, key);
                    ((LeafNode*)t)->Value.insert(((LeafNode*)t)->Value.begin() + i, value);
                    break;
                }
            }
        }
        if(t->Keys.size()>(size_t)(this->n)){
            //split the leaf node
            Node* tnew = new LeafNode();
            tnew->Parent = t->Parent;
            tnew->Keys.insert(tnew->Keys.begin(), t->Keys.begin()+ceil((n+1)/2), t->Keys.end());
            ((LeafNode*)tnew)->Value.insert(((LeafNode*)tnew)->Value.begin(), ((LeafNode*)t)->Value.begin()+ceil((n+1)/2), ((LeafNode*)t)->Value.end());
            t->Keys.erase(t->Keys.begin()+ceil((n+1)/2), t->Keys.end());
            ((LeafNode*)t)->Value.erase(((LeafNode*)t)->Value.begin()+ceil((n+1)/2), ((LeafNode*)t)->Value.end());
            ((LeafNode*)tnew)->Next=((LeafNode*)t)->Next;
            ((LeafNode*)t)->Next=tnew;
            key = t->Keys[ceil((n+1)/2)-1];
            while(t->Parent!=NULL){
                t=t->Parent;
                for(size_t i=0;i<t->Keys.size();++i){
                    if(key>t->Keys.back()){
                        t->Keys.push_back(key);
                        ((In_Node*)t)->Children.push_back(tnew);
                        break;
                    }
                    else if(t->Keys[i]>key){
                        t->Keys.insert(t->Keys.begin() + i, key);
                        ((In_Node*)t)->Children.insert(((In_Node*)t)->Children.begin()+i+1, tnew);
                        break;
                    }
                }
                if(t->Keys.size()>(size_t)(this->n)){
                    Node* nright = new In_Node();
                    nright->Parent = t->Parent;
                    nright->Keys.insert(nright->Keys.begin(), t->Keys.begin()+floor((n+2)/2), t->Keys.end());
                    ((In_Node*)nright)->Children.insert(((In_Node*)nright)->Children.begin(), ((In_Node*)t)->Children.begin()+floor((n+2)/2), ((In_Node*)t)->Children.end());
                    for(size_t i=floor((n+2)/2);i<((In_Node*)t)->Children.size();++i){
                        ((In_Node*)t)->Children[i]->Parent = nright;
                    }
                    key = t->Keys[floor((n+2)/2)-1];
                    t->Keys.erase(t->Keys.begin()+floor((n+2)/2)-1, t->Keys.end());
                    ((In_Node*)t)->Children.erase(((In_Node*)t)->Children.begin()+floor((n+2)/2), ((In_Node*)t)->Children.end());
                    tnew = nright;
                }
                else{
                    tnew->Parent = t;
                    return;
                }


            }
            if(t->Parent==NULL){
                t->Parent = new In_Node();
                ((In_Node*)t->Parent)->Children.insert(((In_Node*)t->Parent)->Children.begin(), t);
                ((In_Node*)t->Parent)->Children.insert(((In_Node*)t->Parent)->Children.begin()+1,tnew);
                if(t->isLeaf){
                    (t->Parent)->Keys.insert((t->Parent)->Keys.begin(),t->Keys.back());
                }
                else{
                    (t->Parent)->Keys.insert((t->Parent)->Keys.begin(),((In_Node*)t)->Children.back()->Keys.back());
                }
                tnew->Parent=t->Parent;
                head = t->Parent;
            }

        }
        else{
            return;
        }
//    cout<<"instert success!"<<endl;
    }


    void remove(int key){
        Node* t=this->head;
        while (!t->isLeaf){ //find the proper location
            int flag = 0;
            for (size_t i=0;i<t->Keys.size();++i){
                if(t->Keys[i]>=key){
                    t = ((In_Node*)t)->Children[i];
                    flag = 1;
                    break;
                }
            }
            if(!flag){
                t = ((In_Node*)t)->Children[t->Keys.size()];
            }
        }
        int flag=0;
        for(size_t i=0;i<t->Keys.size();++i){
            if(t->Keys[i]==key){
                t->Keys.erase(t->Keys.begin()+i);
                ((LeafNode*)t)->Value.erase(((LeafNode*)t)->Value.begin()+i);
                flag=1;
                break;
            }
        }
        if(!flag){
            cout<<"The key you want to delete does not exist!"<<"\n";
            return;
        }
        if(((LeafNode*)t)->Value.size()<ceil((n+1)/2) && t->Parent!=NULL){

            Node* Rsibling;
            Node* Lsibling;
            Rsibling=Lsibling=NULL;

            int Child_num = -1;
            for(size_t i=0;i<((In_Node*)t->Parent)->Children.size();++i){
                if(((In_Node*)t->Parent)->Children[i]==t){
                    Child_num=i;
                    break;
                }
            }
            if(Child_num-1>=0){
                Lsibling = ((In_Node*)t->Parent)->Children[Child_num-1];
            }

            if((size_t)(Child_num+1)<((In_Node*)t->Parent)->Children.size()){
                Rsibling = ((In_Node*)t->Parent)->Children[Child_num+1];
            }

            if(Rsibling!=NULL && ((LeafNode*)Rsibling)->Value.size()-1>=ceil((n+1)/2)){
                t->Keys.push_back(Rsibling->Keys.front());
                ((LeafNode*)t)->Value.push_back(((LeafNode*)Rsibling)->Value.front());
                Rsibling->Keys.erase(Rsibling->Keys.begin());
                ((LeafNode*)Rsibling)->Value.erase(((LeafNode*)Rsibling)->Value.begin());
                t->Parent->Keys[Child_num]=t->Keys.back();
                return;
            }
            else if(Lsibling!=NULL && ((LeafNode*)Lsibling)->Value.size()-1>=ceil((n+1)/2)){
                t->Keys.insert(t->Keys.begin(), Lsibling->Keys.back());
                ((LeafNode*)t)->Value.insert(((LeafNode*)t)->Value.begin(), ((LeafNode*)Lsibling)->Value.back());
                Lsibling->Keys.erase(Lsibling->Keys.end()-1);
                ((LeafNode*)Lsibling)->Value.erase(((LeafNode*)Lsibling)->Value.end()-1);
                t->Parent->Keys[Child_num-1]=Lsibling->Keys.back();
                return;
            }
            else {
                if(Rsibling!=NULL && ((LeafNode*)Rsibling)->Value.size()-1<ceil((n+1)/2)){
                    t->Keys.insert(t->Keys.end(), Rsibling->Keys.begin(), Rsibling->Keys.end());
                    ((LeafNode*)t)->Value.insert(((LeafNode*)t)->Value.end(),((LeafNode*)Rsibling)->Value.begin(), ((LeafNode*)Rsibling)->Value.end());
                    ((LeafNode*)t)->Next=((LeafNode*)Rsibling)->Next;

                    t->Parent->Keys.erase(t->Parent->Keys.begin()+Child_num);
                    ((In_Node*)t->Parent)->Children.erase(((In_Node*)t->Parent)->Children.begin()+Child_num+1);
                }
                else if(Lsibling!=NULL && ((LeafNode*)Lsibling)->Value.size()-1<ceil((n+1)/2)){
                    Lsibling->Keys.insert(Lsibling->Keys.end(), t->Keys.begin(), t->Keys.end());
                    ((LeafNode*)Lsibling)->Value.insert(((LeafNode*)Lsibling)->Value.begin(),((LeafNode*)t)->Value.begin(), ((LeafNode*)t)->Value.end());

                    ((LeafNode*)Lsibling)->Next=((LeafNode*)t)->Next;

                    t->Parent->Keys.erase(t->Parent->Keys.begin()+Child_num-1);
                    ((In_Node*)t->Parent)->Children.erase(((In_Node*)t->Parent)->Children.begin()+Child_num);
                    t = Lsibling;
                }

                while(t->Parent!=this->head){
                    Rsibling=Lsibling=NULL;
                    t=t->Parent;
                    if(((In_Node*)t)->Children.size()>=floor((n+2)/2)){
                        return;
                    }
                    int Child_num = -1;
                    for(size_t i=0;i<((In_Node*)t->Parent)->Children.size();++i){
                        if(((In_Node*)t->Parent)->Children[i]==t){
                            Child_num=i;
                            break;
                        }
                    }
                    if(Child_num-1>=0){
                        Lsibling = ((In_Node*)t->Parent)->Children[Child_num-1];
                    }

                    if((size_t)(Child_num+1)<((In_Node*)t->Parent)->Children.size()){
                        Rsibling = ((In_Node*)t->Parent)->Children[Child_num+1];
                    }
                    if(Rsibling!=NULL && ((In_Node*)Rsibling)->Children.size()-1>=floor((n+2)/2)){
                        ((In_Node*)t)->Children.push_back(((In_Node*)Rsibling)->Children.front());
                        t->Keys.push_back(t->Parent->Keys[Child_num]);
                        t->Parent->Keys[Child_num]=Rsibling->Keys.front();
                        ((In_Node*)Rsibling)->Children.erase(((In_Node*)Rsibling)->Children.begin());
                        Rsibling->Keys.erase(Rsibling->Keys.begin());
                        ((In_Node*)t)->Children.back()->Parent=t;
                        return;
                    }
                    else if(Lsibling!=NULL && ((In_Node*)Lsibling)->Children.size()-1>=floor((n+2)/2)){
                        ((In_Node*)t)->Children.insert(((In_Node*)t)->Children.begin(),((In_Node*)Lsibling)->Children.back());
                        t->Keys.insert(t->Keys.begin(), t->Parent->Keys[Child_num-1]);
                        t->Parent->Keys[Child_num]=Lsibling->Keys.back();
                        ((In_Node*)Lsibling)->Children.erase(((In_Node*)Lsibling)->Children.end()-1);
                        Lsibling->Keys.erase(Lsibling->Keys.end()-1);
                        ((In_Node*)t)->Children.front()->Parent=t;
                        return;
                    }
                    else if(Rsibling!=NULL && ((In_Node*)Rsibling)->Children.size()-1<floor((n+2)/2)){
                        ((In_Node*)Rsibling)->Children.insert(((In_Node*)Rsibling)->Children.begin(),((In_Node*)t)->Children.begin(),((In_Node*)t)->Children.end());
                        Rsibling->Keys.insert(Rsibling->Keys.begin(), t->Parent->Keys[Child_num]);
                        Rsibling->Keys.insert(Rsibling->Keys.begin(), t->Keys.begin(), t->Keys.end());
                        for(size_t i=0;i<((In_Node*)t)->Children.size();++i){
                            ((In_Node*)t)->Children[i]->Parent=Rsibling;
                        }
                        t->Parent->Keys.erase(t->Parent->Keys.begin()+Child_num);
                        ((In_Node*)t->Parent)->Children.erase(((In_Node*)t->Parent)->Children.begin()+Child_num);
                        t = Rsibling;
                    }
                    else if(Lsibling!=NULL && ((In_Node*)Lsibling)->Children.size()-1<floor((n+2)/2)){
                        ((In_Node*)Lsibling)->Children.insert(((In_Node*)Lsibling)->Children.end(), ((In_Node*)t)->Children.begin(), ((In_Node*)t)->Children.end());
                        Lsibling->Keys.insert(Lsibling->Keys.end(), t->Parent->Keys[Child_num-1]);
                        Lsibling->Keys.insert(Lsibling->Keys.end(), t->Keys.begin(), t->Keys.end());
                        for(size_t i=0;i<((In_Node*)t)->Children.size();++i){
                            ((In_Node*)t)->Children[i]->Parent=Lsibling;
                        }
                        t->Parent->Keys.erase(t->Parent->Keys.begin()+Child_num-1);
                        ((In_Node*)t->Parent)->Children.erase(((In_Node*)t->Parent)->Children.begin()+Child_num);
                        t = Lsibling;
                    }
                }
                if(t->Parent==this->head && this->head->Keys.size()==0){
                    this->head=t;
                    return;
                }

            }

        }

    }



    string find(int key){
        Node* t=this->head;
        while (!t->isLeaf){ //find the proper location
            int flag = 0;
            for (size_t i=0;i<t->Keys.size();++i){
                if(t->Keys[i]>=key){
                    t = ((In_Node*)t)->Children[i];
                    flag = 1;
                    break;
                }
            }
            if(!flag){
                t = ((In_Node*)t)->Children[t->Keys.size()];
            }
        }
        LeafNode* temp=(LeafNode*)t;
        for(size_t i=0;i<t->Keys.size();++i){
            if(t->Keys[i]==key){
                return temp->Value[i];
            }
        }
        return "This key does not exist in this B+ tree!";
    }



    int insertSnapValue(int key, string value, In_Node  fathernode) {
        Node *t=this->head;
//        In_Node* rootnode=(In_Node*)t;
        LeafNode* leafNew;
        //处理中间节点
        while(!fathernode.Children[0]->isLeaf){

            int flag = 0;
            for(size_t i=0; i < fathernode.Keys.size(); i++){
                if(fathernode.Keys[i] >= key){
                    flag=1;
//                cout<<"type:"<< typeid((In_Node*)fathernode.Children[i]).name()<<endl;
                    if(!((In_Node*)t)->Children[i]->isExsit){
                        //在该位置替换成新生成的节点
                        In_Node* tnew=new In_Node((In_Node*)(fathernode.Children[i]));
                        ((In_Node*)t)->Children[i]=tnew;
                        fathernode=(In_Node*)(fathernode.Children[i]);
                        fathernode.Parent=tnew->Parent;
                        t=((In_Node*)t)->Children[i];

                    } else{
                        //节点存在则继续往下遍历
                        fathernode=(In_Node*)(fathernode.Children[i]);
                        t=((In_Node*)t)->Children[i];
                    }

                    break;
                }

            }
            if(!flag){
                if(!((In_Node*)t)->Children[fathernode.Keys.size()]->isExsit){
                    //在该位置替换成新生成的节点
                    In_Node* tnew=new In_Node((In_Node*)(fathernode.Children[fathernode.Keys.size()]));
                    ((In_Node*)t)->Children[t->Keys.size()]=tnew;
                    fathernode=(In_Node*)(fathernode.Children[fathernode.Keys.size()]);
                    fathernode.Parent=tnew->Parent;
                    t=((In_Node*)t)->Children[t->Keys.size()];

                } else{
                    //节点存在则继续往下遍历
                    fathernode=(In_Node*)(fathernode.Children[fathernode.Keys.size()]);
                    t=((In_Node*)t)->Children[t->Keys.size()];
                }
            }

        }
        //处理叶子节点
        int leafflag=0;
        for(size_t i=0;i<fathernode.Keys.size();i++){
            if(fathernode.Keys[i]>=key){

                if(!((In_Node*)t)->Children[i]->isExsit){
                    //新创建一个叶子节点并插入到该树中
                    leafNew=new LeafNode((LeafNode*)(fathernode.Children[i]));
                    ((In_Node*)t)->Children[i]=leafNew;
                    leafNew->Parent=t;
                    //叶子节点间的链接
                    //这里有问题，新创建的叶子节点的左右叶子节点该是谁？所以遍历叶子节点的时候不能用next
//                if(i==0){
//                    leafNew->Next=(LeafNode*)(fathernode.Children[1]);
//                } else{
//
//                    LeafNode* temp=(LeafNode*)(fathernode.Children[i-1]);
//                    temp->Next=leafNew;
//                }
                } else{
                    leafNew=(LeafNode*)(((In_Node*)t)->Children[i]);
                }

                leafflag=1;
                break;
            }
        }
        if(!leafflag){
            if(!((In_Node*)t)->Children[t->Keys.size()]->isExsit){
                leafNew=new LeafNode((LeafNode*)(fathernode.Children[t->Keys.size()]));\
            ((In_Node*)t)->Children[t->Keys.size()]=leafNew;
                leafNew->Parent=t;
            }else{
                leafNew=(LeafNode*)(((In_Node*)t)->Children[t->Keys.size()]);

            }
        }
        //将叶子节点中内容修改
        for(size_t i=0;i<leafNew->Keys.size();i++){
            if(leafNew->Keys[i]==key){
//            cout<<"leafnode value(before) is:"<<leafNew->Value[i]<<endl;
                leafNew->Value[i]=value;
//            cout<<"leafnode value(after) is:"<<leafNew->Value[i]<<endl;
                return 0;
            }
        }
        return -1;
    }

    int removeSnapValue(int key){
        Node* t=this->head;
        while (!t->isLeaf){ //find the proper location
            int flag = 0;
            for (size_t i=0;i<t->Keys.size();++i){
                if(t->Keys[i]>=key){
                    t = ((In_Node*)t)->Children[i];
                    flag = 1;
                    break;
                }
            }
            if(!flag){
                t = ((In_Node*)t)->Children[t->Keys.size()];
            }
        }
        for(size_t i=0;i<t->Keys.size();++i){
            if(t->Keys[i]==key){
                ((LeafNode*)t)->Value[i]="w";
                return 0;
            }
        }

        cout<<"This key does not exist in this B+ tree!"<<"\n";
        return 0;
    }

    void printKeys()
    {
        ofstream ofile;
        ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
        if(!ofile.is_open()){
            cout<<"open file error!";
        }
        if(head->Keys.size()==0){
            ofile<<"[]"<<"\n";
            return;
        }
        vector<Node*> q;
        q.push_back(head);
        while(q.size()){
            unsigned long size = q.size();
            for(size_t i=0;i<size;++i){
                if(!q[i]->isLeaf){
                    for(size_t j=0;j<((In_Node*)q[i])->Children.size();++j){
                        q.push_back(((In_Node*)q[i])->Children[j]);
                    }
                }
                ofile<<"[";
                size_t nk;
                for(nk=0;nk<q[i]->Keys.size()-1;++nk){
                    ofile<<q[i]->Keys[nk]<<",";
                }
                ofile<<q[i]->Keys[nk]<<"] ";
            }
            q.erase(q.begin(),q.begin()+size);
            ofile<<"\n";
        }
        ofile.close();

    }
    void printValuesSnap() {
        if(this->head->Keys.size()==0){
            cout<<"[]"<<"\n";
            return;
        }
        vector<Node*> q;
        q.push_back(head);
        while(q.size()){
            unsigned long size = q.size();
            int leafNodeNum=0;
            for(size_t i=0;i<size;++i){
                if(!q[i]->isLeaf){
                    leafNodeNum=((In_Node*)q[i])->Children.size();
                    for(int j=0;j<leafNodeNum;++j){
                        q.push_back(((In_Node*)q[i])->Children[j]);
                    }
                }
            }

            int firstLastInNodePo=q.size()-leafNodeNum;
            LeafNode* tempLeafNode;
            string tempValue;
            for(size_t i=firstLastInNodePo;i<q.size();i++){

                for(size_t j=0;j<((In_Node*)q[i])->Children.size();j++){
                    cout<<"[";
                    tempLeafNode=(LeafNode*)(((In_Node*)q[i])->Children[j]);
                    for(size_t m=0;m<tempLeafNode->Value.size();m++){
                        tempValue=tempLeafNode->Value[m];
                        if(m==tempLeafNode->Value.size()-1){
                            cout<<tempValue;
                        } else{
                            cout<<tempValue<<",";
                        }

                    }
                    cout<<"]";
                }

            }



            q.erase(q.begin(),q.end());

        }
//    for(int i=0;i<q.size();i++){
//        cout<<"[";
//        for(int j=0;j<((LeafNode*)q[i])->Value.size();j++){
//            cout<<((LeafNode*)q[i])->Value[j]<<",";
//        }
//        cout<<"]\n";
//    }

    }

    void printValues()
    {
        ofstream ofile;
        ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
        if(!ofile.is_open()){
            cout<<"open file error!";
        }
        if(head->Keys.size()==0){
            ofile<<"[]"<<"\n";
            return;
        }

        Node* t = this->head;
        while(!t->isLeaf){
            t = ((In_Node*)t)->Children[0];
        }
        while(t!=NULL){
            for(size_t i=0;i<t->Keys.size();++i){
                ofile<<((LeafNode*)t)->Value[i];
            }
            t=((LeafNode*)t)->Next;
        }
        ofile.close();
    }
    string updateValue(int key,string value)
    {
        Node* t=this->head;
        while (!t->isLeaf){ //find the proper location
            int flag = 0;
            for (size_t i=0;i<t->Keys.size();++i){
                if(t->Keys[i]>=key){
                    t = ((In_Node*)t)->Children[i];
                    flag = 1;
                    break;
                }
            }
            if(!flag){
                t = ((In_Node*)t)->Children[t->Keys.size()];
            }
        }
        for(size_t i=0;i<t->Keys.size();++i){
            if(t->Keys[i]==key){
                ((LeafNode*)t)->Value[i]=value;
                return ((LeafNode*)t)->Value[i];
            }
        }
        std::cout<<"do not find the key!"<<"\n";
        return 0;
    }

    Node* getRootNode(){
        return this->head;
    }
    ~BpTree()
    {
        if(this->head){
            delete this->head;
        }
    }


};

#endif /* BpTree_h */
