FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o go-sync ./cmd/server

# ---------------------

FROM alpine:3.19

WORKDIR /app

COPY --from=builder /app/go-sync .

EXPOSE 50051

CMD ["./go-sync"]