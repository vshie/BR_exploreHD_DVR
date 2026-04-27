FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends --no-install-suggests \
    python3 \
    python3-pip \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    ffmpeg \
    exfat-fuse \
    ntfs-3g \
    util-linux \
    zip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir flask requests

WORKDIR /app

RUN mkdir -p /app/recordings

COPY app/ .

ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=main.py

EXPOSE 6010

LABEL version="1.0.26"

ARG IMAGE_NAME
LABEL permissions='\
{\
  "ExposedPorts": {\
    "6010/tcp": {}\
  },\
  "HostConfig": {\
    "Binds": [\
      "/usr/blueos/extensions/br_explorehd_dvr:/app/recordings",\
      "/dev:/dev"\
    ],\
    "ExtraHosts": ["host.docker.internal:host-gateway"],\
    "PortBindings": {\
      "6010/tcp": [\
        {\
          "HostPort": ""\
        }\
      ]\
    },\
    "NetworkMode": "host",\
    "Privileged": true\
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
        "about": "BR_exploreHD_DVR — multi-camera TS recorder for MCM RTSP",\
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
