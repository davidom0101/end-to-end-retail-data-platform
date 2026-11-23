// Route the Redshift hostname to the local SSM listener while keeping it for TLS.
const dns = require('node:dns');

const redshiftHost = process.env.RETAIL_REDSHIFT_HOST;
const lookup = dns.lookup;

if (redshiftHost) {
  dns.lookup = (hostname, ...args) =>
    lookup(hostname === redshiftHost ? '127.0.0.1' : hostname, ...args);
}
