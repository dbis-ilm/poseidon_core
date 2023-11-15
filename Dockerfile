FROM alpine:edge
LABEL Poseidon Graph Database Engine

RUN apk update
RUN apk upgrade
RUN apk add alpine-sdk git make cmake clang clang-dev boost boost-dev python3-dev py3-pip openjdk11
RUN python3 -m venv /home/venv
RUN . /home/venv/bin/activate && pip install antlr4-tools && echo "yes" | antlr4

# Set default user
USER $USER
WORKDIR /home/$USER

RUN cd /home/$USER && git clone https://github.com/dbis-ilm/poseidon_core 
RUN mkdir /home/$USER/poseidon_core/build 
RUN mkdir /home/$USER/poseidon_core/jars 
RUN cp /root/.m2/repository/org/antlr/antlr4/4.13.1/antlr4-4.13.1-complete.jar /home/$USER/poseidon_core/jars

ENV CC=/usr/bin/clang
ENV CXX=/usr/bin/clang++

RUN cd /home/$USER/poseidon_core/build \
     && cmake -DCMAKE_BUILD_TYPE=Debug .. \
     && make

ENTRYPOINT /bin/sh
