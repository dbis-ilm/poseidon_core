FROM dbisilm/llvm_pmdk:clang
LABEL Poseidon Graph Database Engine


# Set default user
USER $USER
WORKDIR /home/$USER

ENV PATH=$PATH:/home/user/.local/bin
 
RUN echo pass | sudo -S mkdir -m 777 -p /mnt/pmem0/poseidon

RUN pip install antlr4-tools && echo "yes" | antlr4

# Download and prepare project
RUN cd /home/$USER \
  && git clone https://github.com/dbis-ilm/poseidon_core \
  && mkdir poseidon_core/build \
  && cd poseidon_core/build \
  && cmake -DCMAKE_BUILD_TYPE=Debug -DLLVM_DIR=/usr/lib64/cmake/llvm .. \
  && make
