# Polaris with a named user whose id matches the Trino image's (1000).
# Needed only while the lake is a shared volume: Trino creates each table's folder and
# Polaris writes metadata into it, so both must be the same user, and the file library
# Polaris uses refuses to run as a user id that has no name.
FROM apache/polaris:1.7.0
USER root
RUN echo 'lake:x:1000:1000:lake:/tmp:/sbin/nologin' >> /etc/passwd \
    && echo 'lake:x:1000:' >> /etc/group
USER 1000:1000
