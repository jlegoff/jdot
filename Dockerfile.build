FROM golang

RUN apt update -y && apt upgrade -y && apt install wget -y

RUN wget https://github.com/open-telemetry/opentelemetry-collector/releases/download/cmd%2Fbuilder%2Fv0.81.0/ocb_0.81.0_linux_arm64 \
  -O /usr/bin/ocb

RUN chmod +x /usr/bin/ocb

WORKDIR /collector
