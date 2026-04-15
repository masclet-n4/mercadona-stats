# ---- Build stage ----
FROM golang:1.22-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# 👇 AQUÍ está la clave
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main ./cmd/worker

# ---- Runtime stage ----
FROM scratch

WORKDIR /app

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/main .

CMD ["./main"]
