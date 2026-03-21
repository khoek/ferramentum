# ocular-tool

Minimal CLI SSO helper for `openconnect` (Cisco AnyConnect-style login).

## Requirements
- Chrome/Chromium (or `--chrome-path`)
- Native build prerequisites for `openconnect-core` / `openconnect-sys` (see crate docs)

## Install
```sh
cargo install ocular-tool
```

Installed command: `ocular`

## Usage
```sh
ocular
ocular --server vpn.example.com/group -- --base-mtu=1370
ocular --server vpn.example.com/group --authenticate shell
ocular --server vpn.example.com/group --authenticate json
ocular --server vpn.example.com/group --only-tunnel 140.247.39.160,example.com
ocular --server vpn.example.com/group --routes replace --only-tunnel 140.247.0.0/16 --only-tunnel example.com
```

## License
AGPL-3.0-only (`LICENSE`).
