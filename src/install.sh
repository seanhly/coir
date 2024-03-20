#!/bin/sh
if ! id librecoir 2>/dev/null; then
	mkdir -p /var/lib/librecoir
	chmod 707 /var/lib/librecoir
	useradd -r -s /bin/bash librecoir -d /var/lib/librecoir
	chown librecoir:librecoir /var/lib/librecoir
fi
mv bin/librecoir /usr/bin
cp src/librecoir.service /etc/systemd/system/
systemctl daemon-reload
