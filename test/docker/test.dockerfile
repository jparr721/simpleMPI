# Use the official Debian image as the base image
FROM golang:1.20

# Install SSH server
RUN apt-get update && \
    apt-get install -y openssh-server && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir /var/run/sshd && \
    mkdir -p /root/.ssh && \
    touch /root/.ssh/authorized_keys && \
    mkdir -p /root/simpleMPI

# Set public key
ARG SSH_PUBLIC_KEY
RUN echo ${SSH_PUBLIC_KEY} > /root/.ssh/authorized_keys

# Copy project into directory
COPY . /root/simpleMPI

WORKDIR /root/simpleMPI

RUN go build test/main.go

# Expose SSH port
EXPOSE 22

# Start SSH server
CMD ["/usr/sbin/sshd", "-D"]
