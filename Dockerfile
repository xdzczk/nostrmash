FROM golang:1.24-alpine AS build

WORKDIR /src
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X 'main.buildVersion=${VERSION}' -X 'main.buildCommit=${COMMIT}' -X 'main.buildTime=${BUILD_TIME}'" -o /out/api ./cmd/api \
	&& CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X 'main.buildVersion=${VERSION}' -X 'main.buildCommit=${COMMIT}' -X 'main.buildTime=${BUILD_TIME}'" -o /out/ingestor ./cmd/ingestor \
	&& CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X 'main.buildVersion=${VERSION}' -X 'main.buildCommit=${COMMIT}' -X 'main.buildTime=${BUILD_TIME}'" -o /out/worker ./cmd/worker

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=build /out/api /app/api
COPY --from=build /out/ingestor /app/ingestor
COPY --from=build /out/worker /app/worker

USER nobody
EXPOSE 8080
CMD ["/app/api"]
