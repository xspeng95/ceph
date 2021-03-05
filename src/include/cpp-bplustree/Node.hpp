
#ifndef Node_hpp
#define Node_hpp

#include <iostream>
#include <string>
#include <vector>
class Node
{
public:
    std::vector<int> Keys;
    Node* Parent;
    bool isLeaf;
    bool isExsit;
};

class LeafNode: public Node
{
public:
    LeafNode(LeafNode *pNode) {
        this->isExsit=true;
        this->isLeaf=pNode->isLeaf;
        for(size_t i=0;i<pNode->Keys.size();++i){
            this->Keys.push_back(pNode->Keys[i]);
        }
        for(size_t i=0;i<pNode->Value.size();++i){
            this->Value.push_back(pNode->Value[i]);

        }
    }

    Node* Next;
    std::vector<std::string> Value;
    LeafNode(){isLeaf=true; Next=NULL; Parent=NULL; isExsit= false;}
};

class In_Node:public Node
{
public:

    In_Node(In_Node *pNode) {
//        cout<<"keys size:"<<pNode->Keys.size()<<endl;
//        cout<<"children size:"<<pNode->Children.size()<<endl;
        for(size_t i=0;i<pNode->Keys.size();++i){
            this->Keys.push_back(pNode->Keys[i]);
        }
        for(size_t i=0;i<pNode->Children.size();++i){
            this->Children.push_back(pNode->Children[i]);

        }
        this->isLeaf=pNode->isLeaf;
        this->isExsit= true;
    }

    std::vector<Node*> Children;
    In_Node(){isLeaf=false; Parent=NULL;isExsit=false; }
    In_Node(const In_Node &node) {
//        cout<<"keys size:"<<node.Keys.size()<<endl;
//        cout<<"children size:"<<node.Children.size()<<endl;
        for(size_t i=0;i<node.Keys.size();++i){
            this->Keys.push_back(node.Keys[i]);
        }
        for(size_t i=0;i<node.Children.size();++i){
            this->Children.push_back(node.Children[i]);

        }
        this->isLeaf=node.isLeaf;
        this->isExsit= true;

    }
};

#endif /* Node_hpp */
