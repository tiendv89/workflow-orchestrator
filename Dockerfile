# syntax=docker/dockerfile:1

# ----- build stage -----
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Cache dependency downloads separately from source builds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build both binaries.
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/orchestrator ./cmd/orchestrator
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/seed ./cmd/seed

# ----- runtime image -----
FROM alpine:3.21

# ca-certificates needed for HTTPS calls to GitHub API.
RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /out/orchestrator .
COPY --from=builder /out/seed .

EXPOSE 8080

ENTRYPOINT ["./orchestrator"]
