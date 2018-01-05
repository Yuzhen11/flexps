# flexps docker example
#
# VERSION 0.1

FROM ubuntu:16.04
ENV DEBIAN_FRONTEND noninteractive

# install packages
RUN apt-get update && apt-get install -y \
    software-properties-common \
    build-essential \
    git-core \
    libboost-all-dev \
    openssh-server \
    cmake \
    libgoogle-perftools-dev \
    libzmq3-dev \
    libxml2 libxml2-dev \
    uuid-dev \
    protobuf-compiler \
    libprotobuf-dev \
    libgsasl7-dev \
    libkrb5-dev 
    
# install libhdfs
# https://github.com/Pivotal-Closed/c-hdfs-client/releases
RUN cd /usr/src/ && wget "https://github.com/Pivotal-Data-Attic/pivotalrd-libhdfs3/archive/v2.2.31.tar.gz" && tar -zxvf v2.2.31.tar.gz
RUN cd /usr/src/c-hdfs-client-2.2.31/ && mkdir -p build && cd build && ../bootstrap --prefix=/usr/local/ && make -j && make install

# Set Core file
RUN ulimit -c unlimited

# install SSHD
RUN mkdir /var/run/sshd

# disable SSH host key checking 
RUN echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

# SSH login fix.
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

# generate an SSH key
RUN /usr/bin/ssh-keygen -f /root/.ssh/id_rsa -t rsa -N ''

# add its ssh keys to authorized_keys
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

RUN service ssh start

#  Build FlexPS
RUN mkdir /root/flexps
WORKDIR /root/flexps
COPY . /root/flexps
RUN cd /root/flexps && mkdir -p debug && cd debug && cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j

EXPOSE 22

# CMD ["/usr/sbin/sshd", "-D"]

