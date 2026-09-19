FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /app .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates wget && \
    adduser -D -u 10001 appuser
COPY --from=build /app /app
USER appuser
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD wget -qO- http://127.0.0.1:8080/health || exit 1
CMD ["/app"]
