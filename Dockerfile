#
# Use docker multistage builds by having intermediate builder image to keep final image size small
# Check https://docs.docker.com/develop/develop-images/multistage-build/ for more details
#
############################
# STEP 1 build executable binary
############################
FROM docker-hub.uncharted.software/golang:alpine AS builder
ARG GITLAB_LOGIN
ARG GITLAB_TOKEN

RUN apk update && apk add --no-cache git && apk add --no-cach make && apk --no-cache add ca-certificates

# add bash + packages to support CGO
RUN apk add bash git build-base     findutils

# Gitlab reads following login information from ~/.netrc file
RUN echo "machine gitlab.uncharted.software login ${GITLAB_LOGIN} password ${GITLAB_TOKEN}" > ~/.netrc
RUN cat ~/.netrc

WORKDIR /go/src/wm-request-queue

COPY . .

RUN make install && make build

############################
# STEP 2 build an image
############################
FROM scratch

# Copy certificate from the builder image. It is required to make https requests
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /go/src/wm-request-queue/wm-request-queue /

ENTRYPOINT ["/wm-request-queue"]

EXPOSE 4040
