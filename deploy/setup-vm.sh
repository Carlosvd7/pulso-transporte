#!/usr/bin/env bash
# Prepara una VM Ubuntu 24.04 (Oracle Cloud Always Free, ARM64) y arranca Pulso del transporte.
# Uso (en la VM):  bash setup-vm.sh <IP_PUBLICA> [full|lite] [REPO_URL]
#   full: pila completa con Redpanda (VM ARM A1, >= 6 GB)
#   lite: un solo proceso sin Redpanda (VM AMD E2.1.Micro, 1 GB)
# Es idempotente: se puede volver a ejecutar para actualizar.
set -euo pipefail

IP="${1:?Uso: bash setup-vm.sh <IP_PUBLICA> [full|lite] [REPO_URL]}"
MODE="${2:-full}"
REPO="${3:-https://github.com/Carlosvd7/pulso-transporte.git}"
case "$MODE" in
  full) COMPOSE=(-f docker-compose.yml -f docker-compose.prod.yml) ;;
  lite) COMPOSE=(-f docker-compose.lite.yml) ;;
  *) echo "Modo desconocido: $MODE (usa full o lite)"; exit 1 ;;
esac
DIR="$HOME/pulso-transporte"

echo "==> Paquetes (Docker de los repositorios de Ubuntu)"
sudo apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  docker.io docker-compose-v2 git iptables-persistent
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"

if [ ! -f /swapfile ] && [ "$(awk '/MemTotal/ {print $2}' /proc/meminfo)" -lt 4000000 ]; then
  echo "==> Poca RAM: creando 2 GB de swap"
  sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile
  sudo mkswap /swapfile && sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
fi

echo "==> Cortafuegos de la VM: las imágenes de Oracle solo dejan pasar el puerto 22"
for port in 80 443; do
  if ! sudo iptables -C INPUT -p tcp --dport "$port" -m state --state NEW -j ACCEPT 2>/dev/null; then
    sudo iptables -I INPUT 5 -p tcp --dport "$port" -m state --state NEW -j ACCEPT
  fi
done
sudo netfilter-persistent save

echo "==> Código"
if [ -d "$DIR/.git" ]; then
  git -C "$DIR" pull --ff-only
else
  git clone "$REPO" "$DIR"
fi
cd "$DIR"

echo "==> Dominio gratuito sin registro: ${IP//./-}.sslip.io apunta a $IP"
echo "PULSO_DOMAIN=${IP//./-}.sslip.io" > .env

mkdir -p data
sudo chown -R 1000:1000 data   # los contenedores escriben como uid 1000

echo "==> Construir y arrancar (la primera vez tarda unos minutos)"
sudo docker compose "${COMPOSE[@]}" up -d --build
sudo docker compose "${COMPOSE[@]}" ps
echo
echo "Listo. Panel: https://${IP//./-}.sslip.io  (el certificado HTTPS tarda ~1 min la primera vez)"
