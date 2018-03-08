# flexps

[![Build Status](https://travis-ci.com/Yuzhen11/flexps.svg?token=q177tztPAL6tTAyRzSkG&branch=master)](https://travis-ci.com/Yuzhen11/flexps)

FlexPS is a minimum Parameter-Server based system, supporting efficient worker-server interaction using various consistency model (BSP/SSP/ASP). 

The introductory paper of FlexPS has been published in PVLDB'18 ([paper](http://www.vldb.org/pvldb/vol11/p566-huang.pdf)). The team for this work consists of Yuzhen Huang, Tatiana Jin, Yidi Wu, Zhenkun Cai, Xiao Yan, Fan Yang, Jinfeng Li, Yuying Guo, James Cheng. We also would like to acknowledge the contributions of Zhanhao Liu, Shuo Tu, Ruoyu Wu to this work.

For paper version, pleace check out [flexps-paper-version](https://github.com/Yuzhen11/flexps-paper-version) which is the research prototype and is not maintained anymore.

In courtesy of the [Husky team](https://github.com/husky-team/) at CUHK. 

Check the wiki pages for FlexPS tutorial:

1. [Build](https://github.com/Yuzhen11/flexps/wiki/Build)
2. [Run](https://github.com/Yuzhen11/flexps/wiki/Run)
3. [Guide](https://github.com/Yuzhen11/flexps/wiki/Flexps-Programming-Guide)
4. [Table](https://github.com/Yuzhen11/flexps/wiki/Table)
5. [HdfsManager](https://github.com/Yuzhen11/flexps/wiki/HdfsManager)

FlexPS has also been used for the csci5570 course project, Fall 2017, CUHK ([course](http://www.cse.cuhk.edu.hk/~jcheng/5570.html), [project](http://appsrv.cse.cuhk.edu.hk/~tjin/csci5570/), [github](https://github.com/TatianaJin/csci5570)). In the course, we gave the students some interfaces, code skeletons, and some tests and let them to implement a Parameter Server step by step. Details can be found [here](https://github.com/TatianaJin/csci5570). Tatiana was the TA of the course. It will be helpful if you want to build a Parameter Server from scratch.  

FlexPS uses gflags, glog, gtest, zmq and libhdfs3 (hdfsmanager only).


Acknowledgement: FlexPS borrows many good design and componenets from [husky](https://github.com/husky-team/husky), [ps-lite](https://github.com/dmlc/ps-lite) and [bosen](https://github.com/sailing-pmls/bosen/).
