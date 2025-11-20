ucc-gen build --ta-version $1
find output/nats -name ".*" -type f -delete
find output/nats -name "*.pyc" -type f -delete
ucc-gen package --path output/nats
