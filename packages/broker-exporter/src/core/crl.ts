/**
 * When the broker's CRL was issued and when it expires (S8-7), read from the PEM file the broker loads. Once
 * `nextUpdate` passes, Mosquitto rejects every client certificate (the CRL is no longer valid), so an always-on
 * broker needs to know days ahead: the exporter publishes both times, and CrlExpiringSoon alerts on them.
 *
 * Pure DER walking, no dependency: CertificateList ::= SEQUENCE { tbsCertList, ... }, and TBSCertList ::= SEQUENCE {
 * version INTEGER OPTIONAL, signature AlgorithmIdentifier, issuer Name, thisUpdate Time, nextUpdate Time OPTIONAL, ... }
 * (RFC 5280, 5.1). Time is UTCTime (YYMMDDHHMMSSZ) or GeneralizedTime (YYYYMMDDHHMMSSZ).
 */

/** One DER element: its tag, and where its content starts and ends. */
interface Tlv {
  tag: number;
  start: number;
  end: number;
}

function readTlv(der: Uint8Array, offset: number): Tlv {
  const tag = der[offset];
  let length = der[offset + 1];
  if (tag === undefined || length === undefined) throw new Error("truncated DER");
  let start = offset + 2;
  if (length & 0x80) {
    const bytes = length & 0x7f;
    if (bytes < 1 || bytes > 4) throw new Error("unsupported DER length");
    length = 0;
    for (let i = 0; i < bytes; i++) length = (length << 8) | (der[start + i] ?? 0);
    start += bytes;
  }
  const end = start + length;
  if (end > der.length) throw new Error("truncated DER");
  return { tag, start, end };
}

/** The elements directly inside a constructed element. */
function children(der: Uint8Array, parent: Tlv): Tlv[] {
  const out: Tlv[] = [];
  for (let offset = parent.start; offset < parent.end;) {
    const child = readTlv(der, offset);
    out.push(child);
    offset = child.end;
  }
  return out;
}

const UTC_TIME = 0x17;
const GENERALIZED_TIME = 0x18;

/** Parses a DER UTCTime or GeneralizedTime (as CAs write them: seconds, then Z). */
function parseTime(der: Uint8Array, tlv: Tlv): Date {
  const text = new TextDecoder().decode(der.subarray(tlv.start, tlv.end));
  const m = tlv.tag === UTC_TIME ? /^(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})Z$/.exec(text) : /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})Z$/.exec(text);
  if (!m) throw new Error(`unexpected CRL time: ${text}`);
  // RFC 5280: a two-digit year below 50 is 20YY, otherwise 19YY.
  const year = tlv.tag === UTC_TIME ? (Number(m[1]) < 50 ? 2000 : 1900) + Number(m[1]) : Number(m[1]);
  return new Date(Date.UTC(year, Number(m[2]) - 1, Number(m[3]), Number(m[4]), Number(m[5]), Number(m[6])));
}

/** A CRL's thisUpdate and nextUpdate (null if the CRL has none, which never expires and should never happen here). */
export function crlTimes(pem: string): { thisUpdate: Date; nextUpdate: Date | null } {
  const body = /-----BEGIN X509 CRL-----([\s\S]+?)-----END X509 CRL-----/.exec(pem)?.[1];
  if (!body) throw new Error("no X509 CRL in the file");
  const der = new Uint8Array(Buffer.from(body.replace(/\s+/g, ""), "base64"));
  const list = readTlv(der, 0);
  const tbs = children(der, list)[0];
  if (!tbs) throw new Error("empty CRL");
  const fields = children(der, tbs);
  let i = fields[0]?.tag === 0x02 ? 1 : 0; // version
  i += 2; // signature AlgorithmIdentifier, issuer Name
  const thisUpdate = fields[i];
  if (!thisUpdate || (thisUpdate.tag !== UTC_TIME && thisUpdate.tag !== GENERALIZED_TIME)) throw new Error("CRL thisUpdate not found");
  const next = fields[i + 1];
  const nextUpdate = next && (next.tag === UTC_TIME || next.tag === GENERALIZED_TIME) ? parseTime(der, next) : null;
  return { thisUpdate: parseTime(der, thisUpdate), nextUpdate };
}
