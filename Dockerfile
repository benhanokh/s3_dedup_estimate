FROM registry.access.redhat.com/ubi8/ubi

WORKDIR /root

RUN dnf update -y && \
    dnf install -y --quiet gcc gcc-c++ gcc-toolset-12 git libcurl-devel cmake zlib-devel openssl-devel

RUN git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp && mkdir sdk_build && cd sdk_build/          && \
    cmake ../aws-sdk-cpp -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=/usr/local/ -DCMAKE_INSTALL_PREFIX=/usr/local/ -DBUILD_ONLY="s3" -DENABLE_TESTING=OFF -DMINIMIZE_SIZE=ON && \
    cmake --build   . --config=Release && \
    cmake --install . --config=Release && \
    rm -rf /root/aws-sdk-cpp           && \
    rm -rf /root/sdk_build/

COPY . s3_dedup_estimate/

RUN cd /root/s3_dedup_estimate && cmake . && make

ENTRYPOINT ["/root/s3_dedup_estimate/s3_dedup_estimate"]

CMD ["--help"]
