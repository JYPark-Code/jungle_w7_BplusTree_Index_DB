/* index_registry.c — index_registry.h 구현.
 *
 * 테이블 이름 기반 선형 검색 배열. MAX_TABLES 한도 내에선 충분히 빠름.
 * 스프린트가 끝나고 규모가 커지면 해시맵으로 교체 예정 (인터페이스 고정).
 */

#include "index_registry.h"

#include <stdlib.h>
#include <string.h>

#define INDEX_REGISTRY_MAX_TABLES 64
#define INDEX_REGISTRY_TABLE_NAME_MAX 64

typedef struct {
    char    table[INDEX_REGISTRY_TABLE_NAME_MAX];
    BPTree *tree;
} RegistryEntry;

static RegistryEntry g_entries[INDEX_REGISTRY_MAX_TABLES];
static int g_entry_count = 0;

static int find_index(const char *table) {
    if (!table) return -1;
    for (int i = 0; i < g_entry_count; ++i) {
        if (strcmp(g_entries[i].table, table) == 0) {
            return i;
        }
    }
    return -1;
}

BPTree *index_registry_get(const char *table) {
    int idx = find_index(table);
    return idx >= 0 ? g_entries[idx].tree : NULL;
}

BPTree *index_registry_get_or_create(const char *table, int order) {
    if (!table) return NULL;
    int idx = find_index(table);
    if (idx >= 0) return g_entries[idx].tree;

    if (g_entry_count >= INDEX_REGISTRY_MAX_TABLES) return NULL;

    size_t name_len = strlen(table);
    if (name_len + 1 > INDEX_REGISTRY_TABLE_NAME_MAX) return NULL;

    BPTree *tree = bptree_create(order);
    if (!tree) return NULL;

    RegistryEntry *e = &g_entries[g_entry_count];
    memcpy(e->table, table, name_len + 1);
    e->tree = tree;
    ++g_entry_count;
    return tree;
}

void index_registry_destroy_all(void) {
    for (int i = 0; i < g_entry_count; ++i) {
        bptree_destroy(g_entries[i].tree);
        g_entries[i].tree = NULL;
        g_entries[i].table[0] = '\0';
    }
    g_entry_count = 0;
}
