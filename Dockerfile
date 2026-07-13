FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends --no-install-suggests \
    python3 \
    python3-pip \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir flask requests

WORKDIR /app

RUN mkdir -p /app/recordings

COPY app/ .

ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=main.py

EXPOSE 4444

LABEL version="1.0.41"

ARG IMAGE_NAME
LABEL permissions='\
{\
  "ExposedPorts": {\
    "4444/tcp": {}\
  },\
  "HostConfig": {\
    "Binds": [\
      "/usr/blueos/extensions/br_explorehd_dvr:/app/recordings"\
    ],\
    "ExtraHosts": ["host.docker.internal:host-gateway"],\
    "PortBindings": {\
      "4444/tcp": [\
        {\
          "HostPort": ""\
        }\
      ]\
    },\
    "NetworkMode": "host"\
  }\
}'

ARG AUTHOR
ARG AUTHOR_EMAIL
LABEL authors='[\
    {\
        "name": "Blue Robotics",\
        "email": "support@bluerobotics.com"\
    }\
]'

ARG MAINTAINER
ARG MAINTAINER_EMAIL
LABEL company='\
{\
        "about": "BR_exploreHD_DVR — cloud RTMP relay + Live view for MCM RTSP",\
        "name": "Blue Robotics",\
        "email": "support@bluerobotics.com"\
    }'
LABEL type="tool"

ARG REPO
ARG OWNER
LABEL readme='https://github.com/bluerobotics'
LABEL links='\
{\
        "source": "https://github.com/bluerobotics"\
    }'
LABEL requirements="core >= 1.3"

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
