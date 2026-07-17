# syntax=docker/dockerfile:1.7
FROM golang:1.26.5-alpine AS build

WORKDIR /src
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
ARG GOPROXY=https://proxy.golang.org,direct
ARG GOSUMDB=sum.golang.org

ENV GOPROXY=${GOPROXY}
ENV GOSUMDB=${GOSUMDB}

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	sh -ec 'for i in 1 2 3; do go mod download && exit 0; echo "go mod download failed, retry ${i}/3"; sleep $((i*2)); done; exit 1'

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X 'main.buildVersion=${VERSION}' -X 'main.buildCommit=${COMMIT}' -X 'main.buildTime=${BUILD_TIME}'" -o /out/api ./cmd/api \
	&& CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X 'main.buildVersion=${VERSION}' -X 'main.buildCommit=${COMMIT}' -X 'main.buildTime=${BUILD_TIME}'" -o /out/ingestor ./cmd/ingestor \
	&& CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X 'main.buildVersion=${VERSION}' -X 'main.buildCommit=${COMMIT}' -X 'main.buildTime=${BUILD_TIME}'" -o /out/worker ./cmd/worker \
	&& CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X 'main.buildVersion=${VERSION}' -X 'main.buildCommit=${COMMIT}' -X 'main.buildTime=${BUILD_TIME}'" -o /out/trust_worker ./cmd/trust_worker

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=build /out/api /app/api
COPY --from=build /out/ingestor /app/ingestor
COPY --from=build /out/worker /app/worker
COPY --from=build /out/trust_worker /app/trust_worker

USER nobody
EXPOSE 8080
CMD ["/app/api"]
