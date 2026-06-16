FROM wiremock/wiremock:3.9.1

COPY mappings /home/wiremock/mappings

EXPOSE 8080

ENTRYPOINT ["/docker-entrypoint.sh", "--port", "8080", "--verbose"]
