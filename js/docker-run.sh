# Run a container from image "gaarf" (see docker-build.sh) with passing parameters:
sudo docker run --mount type=bind,source="$(pwd)/.gaarfrc",target=/app/.gaarfrc \
  --mount type=bind,source="$(pwd)/ads-scripts",target=/app/ads-scripts \
  --mount type=bind,source="$(pwd)/service_account.json",target=/app/service_account.json \
  gaarf ./ads-scripts/*.sql