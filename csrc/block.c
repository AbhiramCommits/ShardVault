#include "block.h"

#include <stdatomic.h>
#include <string.h>

#define SV_CRC32C_POLY UINT32_C(0x82F63B78)

_Static_assert(sizeof(sv_block_header_t) == SV_BLOCK_HEADER_SIZE,
               "sv_block_header_t must be exactly 24 bytes");
_Static_assert(offsetof(sv_block_header_t, lsn) == 0, "lsn at offset 0");
_Static_assert(offsetof(sv_block_header_t, payload_len) == 8,
               "payload_len at offset 8");
_Static_assert(offsetof(sv_block_header_t, crc32c) == 12, "crc32c at offset 12");
_Static_assert(offsetof(sv_block_header_t, flags) == 16, "flags at offset 16");
_Static_assert(offsetof(sv_block_header_t, pad) == 17, "pad at offset 17");

static atomic_int sv_crc_tables_ready = 0;
static atomic_flag sv_crc_tables_lock = ATOMIC_FLAG_INIT;
static uint32_t sv_crc_tables[8][256];

static void sv_crc32c_ensure_tables(void) {
    if (atomic_load_explicit(&sv_crc_tables_ready, memory_order_acquire)) {
        return;
    }
    while (atomic_flag_test_and_set_explicit(&sv_crc_tables_lock,
                                             memory_order_acquire)) {
    }
    if (!atomic_load_explicit(&sv_crc_tables_ready, memory_order_relaxed)) {
        for (uint32_t b = 0; b < 256u; b++) {
            uint32_t r = b;
            for (unsigned k = 0; k < 8; k++) {
                r = (r >> 1) ^ (SV_CRC32C_POLY & (0u - (r & 1u)));
            }
            sv_crc_tables[0][b] = r;
        }
        for (unsigned k = 1; k < 8; k++) {
            for (uint32_t b = 0; b < 256u; b++) {
                uint32_t r = sv_crc_tables[k - 1][b];
                sv_crc_tables[k][b] =
                    (r >> 8) ^ sv_crc_tables[0][r & 0xffu];
            }
        }
        atomic_store_explicit(&sv_crc_tables_ready, 1, memory_order_release);
    }
    atomic_flag_clear_explicit(&sv_crc_tables_lock, memory_order_release);
}

static uint32_t sv_crc32c_update(uint32_t crc, const uint8_t *buf, size_t len) {
    const uint32_t(*tbl)[256] = sv_crc_tables;
    while (len >= 8u) {
        uint64_t w = 0;
        for (unsigned i = 0; i < 8; i++) {
            w |= (uint64_t)buf[i] << (8u * i);
        }
        uint64_t c = (uint64_t)crc ^ w;
        crc = (uint32_t)(tbl[7][c & 0xffu] ^
                         tbl[6][(c >> 8) & 0xffu] ^
                         tbl[5][(c >> 16) & 0xffu] ^
                         tbl[4][(c >> 24) & 0xffu] ^
                         tbl[3][(c >> 32) & 0xffu] ^
                         tbl[2][(c >> 40) & 0xffu] ^
                         tbl[1][(c >> 48) & 0xffu] ^
                         tbl[0][(c >> 56) & 0xffu]);
        buf += 8;
        len -= 8;
    }
    while (len--) {
        crc = tbl[0][(crc ^ *buf++) & 0xffu] ^ (crc >> 8);
    }
    return crc;
}

uint32_t sv_crc32c(const uint8_t *buf, size_t len) {
    sv_crc32c_ensure_tables();
    return ~sv_crc32c_update(~UINT32_C(0), buf, len);
}

static uint64_t sv_get_u64le(const uint8_t *p) {
    uint64_t v = 0;
    for (unsigned i = 0; i < 8; i++) {
        v |= (uint64_t)p[i] << (8u * i);
    }
    return v;
}

static uint32_t sv_get_u32le(const uint8_t *p) {
    uint32_t v = 0;
    for (unsigned i = 0; i < 4; i++) {
        v |= (uint32_t)p[i] << (8u * i);
    }
    return v;
}

static void sv_put_u64le(uint8_t *p, uint64_t v) {
    for (unsigned i = 0; i < 8; i++) {
        p[i] = (uint8_t)(v >> (8u * i));
    }
}

static void sv_put_u32le(uint8_t *p, uint32_t v) {
    for (unsigned i = 0; i < 4; i++) {
        p[i] = (uint8_t)(v >> (8u * i));
    }
}

static uint32_t sv_block_checksum(const uint8_t blk[SV_BLOCK_SIZE],
                                  uint32_t len) {
    static const uint8_t zeros[4] = {0, 0, 0, 0};
    uint32_t crc = ~UINT32_C(0);
    crc = sv_crc32c_update(crc, blk, 12);
    crc = sv_crc32c_update(crc, zeros, sizeof(zeros));
    crc = sv_crc32c_update(crc, blk + 16, 8);
    crc = sv_crc32c_update(crc, blk + SV_BLOCK_HEADER_SIZE, len);
    return ~crc;
}

int sv_block_encode(uint8_t out[SV_BLOCK_SIZE], uint64_t lsn,
                    const uint8_t *payload, uint32_t len, uint8_t flags) {
    if (out == NULL || (payload == NULL && len != 0u)) {
        return SV_ERR_INVAL;
    }
    if (len > SV_BLOCK_PAYLOAD_MAX) {
        return SV_ERR_LEN;
    }
    sv_crc32c_ensure_tables();
    memset(out, 0, SV_BLOCK_SIZE);
    sv_put_u64le(out + 0, lsn);
    sv_put_u32le(out + 8, len);
    out[16] = flags;
    memcpy(out + SV_BLOCK_HEADER_SIZE, payload, len);
    sv_put_u32le(out + 12, sv_block_checksum(out, len));
    return SV_OK;
}

int sv_block_decode(const uint8_t in[SV_BLOCK_SIZE], sv_block_t *out) {
    if (in == NULL || out == NULL) {
        return SV_ERR_INVAL;
    }
    uint32_t len = sv_get_u32le(in + 8);
    if (len > SV_BLOCK_PAYLOAD_MAX) {
        memset(out, 0, sizeof(*out));
        return SV_ERR_LEN;
    }
    sv_crc32c_ensure_tables();
    if (sv_block_checksum(in, len) != sv_get_u32le(in + 12)) {
        memset(out, 0, sizeof(*out));
        return SV_ERR_CRC;
    }
    out->lsn = sv_get_u64le(in);
    out->payload_len = len;
    out->flags = in[16];
    memcpy(out->payload, in + SV_BLOCK_HEADER_SIZE, len);
    return SV_OK;
}
