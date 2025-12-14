FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY /bin .
RUN chmod +x ./raft && mkdir -p /data
EXPOSE 8000
ENTRYPOINT ["./raft"]