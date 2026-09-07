# Coolify bind-mounts compose volumes from the persistent app directory,
# which does not contain this repo's files. Baking the config into the
# image avoids mounting a missing host path (Docker then creates a
# directory named nginx.conf and the container fails to start).
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/nginx.conf
