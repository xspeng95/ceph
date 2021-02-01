// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/Shell.h"
//tianjia
#include <fstream>
#include <iostream>
using namespace std;

int main(int argc, const char **argv)
{
    ofstream ofile;
    ofile.open("/home/xspeng/Desktop/alisnap/myceph.log",ios::app);
    if(!ofile.is_open()){
        cout<<"open file error!";
    }
    ofile<<"come to tools::rbd::rbd.cc::main()|| argv:"<<argv<<"\n";
    ofile.close();
  rbd::Shell shell;
  return shell.execute(argc, argv);
}
