#include "block.h"

#include <stdio.h>
#include <string.h>

static int failures = 0;

#define CHECK(cond, name)                                                     \
    do {                                                                      \
        if (!(cond)) {                                                        \
            printf("FAIL: %s (%s:%d)\n", name, __FILE__, __LINE__);           \
            failures++;                                                       \
        }                                                                     \
    } while (0)

int main(void) {
    CHECK(sv_crc32c((const uint8_t *)"123456789", 9) ==
              UINT32_C(0xE3069283),
          "crc32c known vector");
    CHECK(sv_crc32c(NULL, 0) == 0, "crc32c empty input");
    CHECK(SV_BLOCK_SIZE == 4096, "block size constant");
    CHECK(SV_BLOCK_HEADER_SIZE == 24, "header size constant");
    CHECK(SV_BLOCK_PAYLOAD_MAX == 4072, "payload max constant");
    CHECK(sizeof(sv_block_header_t) == 24, "on-disk header layout");

    uint8_t payload[777];
    for (size_t i = 0; i < sizeof(payload); i++) {
        payload[i] = (uint8_t)(i * 31u + 7u);
    }

    uint8_t blk[SV_BLOCK_SIZE];
    CHECK(sv_block_encode(blk, UINT64_C(0x1122334455667788), payload,
                          (uint32_t)sizeof(payload), 0x5Au) == SV_OK,
          "encode roundtrip block");
    CHECK(blk[17] == 0 && blk[18] == 0 && blk[19] == 0, "pad bytes zeroed");

    sv_block_t dec;
    CHECK(sv_block_decode(blk, &dec) == SV_OK, "decode roundtrip block");
    CHECK(dec.lsn == UINT64_C(0x1122334455667788), "decoded lsn");
    CHECK(dec.payload_len == sizeof(payload), "decoded payload_len");
    CHECK(dec.flags == 0x5Au, "decoded flags");
    CHECK(memcmp(dec.payload, payload, sizeof(payload)) == 0,
          "decoded payload");

    blk[SV_BLOCK_HEADER_SIZE + 400] ^= 0x01u;
    CHECK(sv_block_decode(blk, &dec) == SV_ERR_CRC,
          "payload corruption detected");
    blk[SV_BLOCK_HEADER_SIZE + 400] ^= 0x01u;
    blk[3] ^= 0x01u;
    CHECK(sv_block_decode(blk, &dec) == SV_ERR_CRC,
          "header corruption detected");
    blk[3] ^= 0x01u;
    CHECK(sv_block_decode(blk, &dec) == SV_OK, "block decodes after repair");

    CHECK(sv_block_encode(blk, 1, payload, SV_BLOCK_PAYLOAD_MAX + 1u, 0) ==
              SV_ERR_LEN,
          "encode rejects oversize payload");

    uint8_t crafted[SV_BLOCK_SIZE];
    memset(crafted, 0, sizeof(crafted));
    uint32_t bad_len = SV_BLOCK_PAYLOAD_MAX + 1u;
    for (unsigned i = 0; i < 4; i++) {
        crafted[8 + i] = (uint8_t)(bad_len >> (8u * i));
    }
    CHECK(sv_block_decode(crafted, &dec) == SV_ERR_LEN,
          "decode rejects oversize payload_len field");

    uint8_t max_payload[SV_BLOCK_PAYLOAD_MAX];
    for (size_t i = 0; i < sizeof(max_payload); i++) {
        max_payload[i] = (uint8_t)(i >> 1);
    }
    CHECK(sv_block_encode(blk, UINT64_C(0xFFFFFFFFFFFFFFFF), max_payload,
                          (uint32_t)sizeof(max_payload), 0xFFu) == SV_OK,
          "encode max-size payload");
    CHECK(sv_block_decode(blk, &dec) == SV_OK, "decode max-size payload");
    CHECK(dec.payload_len == sizeof(max_payload), "max-size payload_len");
    CHECK(memcmp(dec.payload, max_payload, sizeof(max_payload)) == 0,
          "max-size payload content");

    CHECK(sv_block_encode(blk, 7, NULL, 0, 0) == SV_OK, "encode empty payload");
    CHECK(sv_block_decode(blk, &dec) == SV_OK, "decode empty payload");
    CHECK(dec.payload_len == 0, "empty payload_len");

    if (failures != 0) {
        printf("%d test(s) failed\n", failures);
        return 1;
    }
    printf("all C tests passed\n");
    return 0;
}
