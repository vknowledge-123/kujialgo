# Koju Multi-Service Deployment

The app now runs as three independent systemd services:

- `kujialgo-api`: FastAPI dashboard and control API on `127.0.0.1:8000`
- `kujialgo-engine`: Dhan market feed, order socket, strategy evaluation, order placement
- `kujialgo-reconcile`: premarket cache, missing candle recovery, broker/order/trade-book reconciliation

Install or update services on the VM:

```bash
cd /opt/kujialgo
sudo cp deploy/systemd/kujialgo-api.service /etc/systemd/system/
sudo cp deploy/systemd/kujialgo-engine.service /etc/systemd/system/
sudo cp deploy/systemd/kujialgo-reconcile.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl disable --now kujialgo || true
sudo systemctl enable --now kujialgo-api kujialgo-engine kujialgo-reconcile
```

Check status:

```bash
sudo systemctl status kujialgo-api kujialgo-engine kujialgo-reconcile --no-pager
sudo journalctl -u kujialgo-api -f
sudo journalctl -u kujialgo-engine -f
sudo journalctl -u kujialgo-reconcile -f
```
