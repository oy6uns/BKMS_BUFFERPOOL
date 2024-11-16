#ifndef DB_BUFFER_H_
#define DB_BUFFER_H_

#include "page.h"

#include <iostream>
#include <memory>
#include <string>
#include <stdexcept>

template<typename ... Args>
std::string string_format( const std::string& format, Args ... args )
{
    int size_s = std::snprintf(nullptr, 0, format.c_str(), args ... ) + 1; // Extra space for '\0'

    if(size_s <= 0) 
        throw std::runtime_error( "Error during formatting." );

    auto size = static_cast<size_t>(size_s);
    std::unique_ptr<char[]> buf(new char[size]);
    std::snprintf(buf.get(), size, format.c_str(), args ...);

    return std::string(buf.get(), buf.get() + size - 1); // We don't want the '\0' inside
}

#define MAX_USAGE_COUNT (5)

// For stat
extern int64_t stat_get_buffer;

typedef struct buf_descriptor_t {
    int64_t table_id;
    pagenum_t page_num;
    page_t *buf_page;
//  TODO -----------------------------------------------------------------------
    int reference_count;
    int usage_count;
    bool is_dirty;
//  ----------------------------------------------------------------------------
} buf_descriptor_t;

typedef struct ht_entry_t {
    buf_descriptor_t *buf_desc;
    // 해시 충돌 해결을 위한 체이닝 포인터
    ht_entry_t *next;
} ht_entry_t;

typedef struct hashtable_t {
    uint32_t num_ht_entries;
    ht_entry_t *ht_entries;
} hashtable_t;

// linked list형의 free_list 구현
typedef struct free_list_node {
    buf_descriptor_t* buf_desc;
    free_list_node* next;
} free_list_node;

typedef struct buffer_pool_t {
    uint32_t num_buf;
    hashtable_t hashtable;
//  TODO -----------------------------------------------------------------------
    // buf_descriptor와 buf_page들의 일대일 대응 배열
    buf_descriptor_t *buf_descriptors;
    page_t *buf_pages;

    // clock 알고리즘의 marker
    int clock_hand;

    // linked list의 head를 가리키는 포인터
    free_list_node* free_list_head;
//  ----------------------------------------------------------------------------
} buffer_pool_t;

void mark_buffer_dirty(buf_descriptor_t *buf_desc);
void unpin_buffer(buf_descriptor_t *buf_desc);

int64_t buffer_open_table(const char *pathname);
int init_buffer_pool(uint32_t num_ht_entries, uint32_t num_buf);
buf_descriptor_t *get_buffer(int64_t table_id, pagenum_t page_num);
buf_descriptor_t *get_buffer_of_new_page(int64_t table_id);
void free_page(int64_t table_id, buf_descriptor_t *free_buf);
int close_buffer_pool();

// freelist 초기화 함수 선언
void init_freelist();
void add_to_freelist(buf_descriptor_t *buf_desc);
buf_descriptor_t *get_from_freelist();

// For stat
void init_buffer_stat();
int64_t get_buffer_hit_ratio();
std::string get_buffer_stat();
void print_buffer_stat();

#endif // DB_BUFFER_H