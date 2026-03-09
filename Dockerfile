FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl wget ca-certificates gnupg2 apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Install .NET 10 preview runtime
RUN curl -sSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh \
    && chmod +x /tmp/dotnet-install.sh \
    && /tmp/dotnet-install.sh --channel 10.0 --runtime aspnetcore --install-dir /usr/share/dotnet \
    && ln -sf /usr/share/dotnet/dotnet /usr/bin/dotnet \
    && rm /tmp/dotnet-install.sh

# Install Redpanda
RUN curl -1sLf 'https://dl.redpanda.com/nFiirAGP3K4IC4Gh/redpanda/cfg/setup/bash.deb.sh' | bash \
    && apt-get install -y redpanda \
    && rm -rf /var/lib/apt/lists/*

# Copy published .NET app
WORKDIR /app
COPY publish/ .

# Copy and set up startup script
COPY start.sh /start.sh
RUN chmod +x /start.sh

EXPOSE 5218

CMD ["/start.sh"]
