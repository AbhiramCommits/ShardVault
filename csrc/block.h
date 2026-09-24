#ifndef SHARDVAULT_BLOCK_H
#define SHARDVAULT_BLOCK_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SV_BLOCK_SIZE 4096u
#define SV_BLOCK_HEADER_SIZE 24u
#define SV_BLOCK_PAYLOAD_MAX (SV_BLOCK_SIZE - SV_BLOCK_HEADER_SIZE)

enum {
    SV_OK = 0,
    SV_ERR_INVAL = -1,
    SV_ERR_CRC = -2,
    SV_ERR_LEN = -3
};

typedef struct {
    uint64_t lsn;
    uint32_t payload_len;
    uint32_t crc32c;
    uint8_t flags;
    uint8_t pad[3];
} sv_block_header_t;

typedef struct {
    uint64_t lsn;
    uint32_t payload_len;
    uint8_t flags;
    uint8_t payload[SV_BLOCK_PAYLOAD_MAX];
} sv_block_t;

uint32_t sv_crc32c(const uint8_t *buf, size_t len);

int sv_block_encode(uint8_t out[SV_BLOCK_SIZE], uint64_t lsn,
                    const uint8_t *payload, uint32_t len, uint8_t flags);

int sv_block_decode(const uint8_t in[SV_BLOCK_SIZE], sv_block_t *out);

#ifdef __cplusplus
}
#endif

#endif /* SHARDVAULT_BLOCK_H */
