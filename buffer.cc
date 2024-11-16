#include "buffer.h"
#include "file.h"

// For stats
int64_t stat_get_buffer;

buffer_pool_t buffer_pool;

void inline flush_buffer(buf_descriptor_t *buf_desc) {
    std::cout << "Entering flush_buffer with buf_desc: " << buf_desc << std::endl;
    if (buf_desc == nullptr) {
        std::cerr << "Error: buf_desc is nullptr in flush_buffer." << std::endl;
        return;
    }
    if (buf_desc->buf_page == nullptr) {
        std::cerr << "Error: buf_desc->buf_page is nullptr in flush_buffer." << std::endl;
        return;
    }
    file_write_page(buf_desc->table_id, buf_desc->page_num, buf_desc->buf_page);   
    std::cout << "Exiting flush_buffer" << std::endl;
}

int64_t buffer_open_table(const char *pathname) {
    std::cout << "Entering buffer_open_table with pathname: " << pathname << std::endl;
    std::cout << "Exiting buffer_open_table with table_id: " << file_open_table_file(pathname) << std::endl;
    return file_open_table_file(pathname);
}

// MARK - dirty 상태를 표시하여, 나중에 flush할 수 있게 해준다. 
void inline mark_buffer_dirty(buf_descriptor_t *buf_desc) {
    std::cout << "Marking buffer as dirty: " << buf_desc << std::endl;
//  TODO -----------------------------------------------------------------------
    // flush_buffer(buf_desc);
    if (buf_desc == nullptr) {
        std::cerr << "Error: buf_desc is nullptr in mark_buffer_dirty." << std::endl;
        return;
    }
    buf_desc->is_dirty = true;
//  ----------------------------------------------------------------------------
}

void inline pin_buffer(buf_descriptor_t *buf_desc) {
//  TODO -----------------------------------------------------------------------
    std::cout << "Pinning buffer: " << buf_desc << std::endl;
    if (buf_desc == nullptr) {
        std::cerr << "Error: buf_desc is nullptr in pin_buffer." << std::endl;
        return;
    }
    if (buf_desc->usage_count < 0) {
        std::cerr << "Error: Invalid usage_count value for buf_desc: " << buf_desc << std::endl;
    }
    // 페이지가 참조 중임을 표시
    buf_desc->reference_count = 1;
    // 최댓값을 넘진 않는 선에서 사용 횟수 증가
    buf_desc->usage_count = std::min(buf_desc->usage_count + 1, MAX_USAGE_COUNT);
//  ----------------------------------------------------------------------------
}

// MARK - freelist에 buf_descriptor 추가
void add_to_freelist(buf_descriptor_t *buf_desc) {
    if (buf_desc == nullptr) {
        std::cerr << "Attempted to add a nullptr buf_desc to freelist." << std::endl;
        return;
    }
    free_list_node* new_node = new free_list_node{buf_desc, buffer_pool.free_list_head};

    if (new_node == nullptr) { // 추가된 검사
        std::cerr << "Error: Failed to allocate memory for new freelist node." << std::endl;
        return;
    }
    buffer_pool.free_list_head = new_node;
}

// MARK - freelist 초기화 
void init_freelist() {
    std::cout << "Initializing freelist" << std::endl;
    buffer_pool.free_list_head = nullptr;
    int count = 0;
    for (uint32_t i = 0; i<buffer_pool.num_buf; i++) {
        add_to_freelist(&buffer_pool.buf_descriptors[i]);
        count++;
    }
    std::cout << "freelist에 " << count << "개 buf_descriptor 추가" << std::endl;
}

// MARK - freelist에서 buf_descriptor 반환
buf_descriptor_t* get_from_freelist() {
    std::cout << "Getting from freelist" << std::endl;
    // freelist가 비어 있으면 nullptr 반환
    if (buffer_pool.free_list_head == nullptr) {
        std::cerr << "Error: freelist is empty." << std::endl;
        return nullptr;
    }

    free_list_node* free_node = buffer_pool.free_list_head;
    buffer_pool.free_list_head = free_node->next;

    buf_descriptor_t* buf_desc = free_node->buf_desc;
    std::cout << "Returning from freelist: " << buf_desc << std::endl;
    delete free_node;

    return buf_desc;
}


void inline unpin_buffer(buf_descriptor_t *buf_desc) {
//  TODO -----------------------------------------------------------------------
    std::cout << "Unpinning buffer: " << buf_desc << std::endl;
    if (buf_desc == nullptr) {
        std::cerr << "Error: buf_desc is nullptr in unpin_buffer." << std::endl;
        return;
    }
    // 페이지 참조를 해제
    buf_desc->reference_count = 0;

    // 해제된 페이지를 freelist에 추가
    add_to_freelist(buf_desc);

    // free(buf_desc->buf_page);
    // free(buf_desc);
//  ----------------------------------------------------------------------------
}

/**
 * @brief Initialize the hashtable of the buffer pool.
 * 
 * @param num_ht_entries The number of hashtable entries
 * 
 * @details Initialize the hashtable using num_ht_entries. 
 */
void init_hashtable(uint32_t num_ht_entries) {
    std::cout << "Initializing hashtable with num_ht_entries: " << num_ht_entries << std::endl;
    buffer_pool.hashtable.num_ht_entries = num_ht_entries;
//  TODO -----------------------------------------------------------------------
    buffer_pool.hashtable.ht_entries = new ht_entry_t[num_ht_entries];

    if (buffer_pool.hashtable.ht_entries == nullptr) {
        std::cerr << "Error: Failed to allocate memory for hashtable entries." << std::endl;
        return;
    }
    
    for (uint32_t i = 0; i < num_ht_entries; i++) {
        buffer_pool.hashtable.ht_entries[i].buf_desc = nullptr;
        buffer_pool.hashtable.ht_entries[i].next = nullptr;
    }
//  ----------------------------------------------------------------------------
    
}

/**
 * @brief Initialize the buffer pool.
 * 
 * @param num_ht_entries The number of hashtable entries
 * @param num_buf The number of buffer (descriptor and page)
 * @retval 0: successful
 * @retval others: failed
 * 
 * @details The num_buf must be greater or equal than 4 
 * (The splitting, deleting operation pins 3 page at once) + (header page)
 */
int init_buffer_pool(uint32_t num_ht_entries, uint32_t num_buf) {
    // buf_descriptor_t *buf;
    std::cout << "Initializing buffer pool with num_ht_entries: " << num_ht_entries << ", num_buf: " << num_buf << std::endl;

    if (num_buf < 4)
        return 1;

    if (init_tables())
        return 1;

    init_hashtable(num_ht_entries);

//  TODO -----------------------------------------------------------------------
    buffer_pool.num_buf = num_buf;
    buffer_pool.buf_descriptors = new buf_descriptor_t[num_buf];
    buffer_pool.buf_pages = new page_t[num_buf];

    buffer_pool.clock_hand = 0;
    if (buffer_pool.buf_descriptors == nullptr || buffer_pool.buf_pages == nullptr) {
        std::cerr << "Error: Failed to allocate memory for buffer pool." << std::endl;
        return 1;
    }

    // freelist 초기화 및 각 buf_descriptor의 buf_page 메모리 할당
    for (uint32_t i = 0; i < num_buf; i++) {
        buffer_pool.buf_descriptors[i].table_id = -1;        // 비유효 값
        buffer_pool.buf_descriptors[i].page_num = -1;        // 비유효 값
        buffer_pool.buf_descriptors[i].buf_page = &buffer_pool.buf_pages[i]; // 페이지 메모리 연결
        buffer_pool.buf_descriptors[i].reference_count = 0;  // 참조되지 않음
        buffer_pool.buf_descriptors[i].usage_count = 0;      // 사용되지 않음
        buffer_pool.buf_descriptors[i].is_dirty = false;     // 수정되지 않음
    }

    init_freelist();
//  ----------------------------------------------------------------------------
    init_buffer_stat();
    std::cout << "Buffer pool initialized successfully" << std::endl;

    return 0;
}

// macros for hashtable
//  TODO -----------------------------------------------------------------------

/**
 * @brief 
 * If necessary, modify these functions or implement an additional functions
 * of your own.
 */

#define hash(table_id, page_num) \
    ((((uint64_t)table_id) * 100000 + page_num)%(buffer_pool.hashtable.num_ht_entries))
#define get_ht_entry(table_id, page_num) \
    (&(buffer_pool.hashtable.ht_entries[hash(table_id, page_num)]))

//  ----------------------------------------------------------------------------

/**
 * @brief Look up the buffer(page) in hashtable.
 * 
 * @return The memory address of the found buffer descriptor.
 */
inline buf_descriptor_t *hashtable_lookup(int64_t table_id, pagenum_t page_num) {
    // hashtable로 부터 일치하는 buf_descriptor를 불러옴
    ht_entry_t *ht_entry = get_ht_entry(table_id, page_num);
    if (ht_entry == nullptr) { // 추가된 검사
        std::cerr << "Error: hashtable entry is nullptr in hashtable_lookup." << std::endl;
        return nullptr;
    }

    // buf_descriptor_t *buf_desc;
    std::cout << "Looking up hashtable entry for table_id: " << table_id << ", page_num: " << page_num << ", hash entry: " << ht_entry << std::endl;

//  TODO -----------------------------------------------------------------------
    // 해당 entry의 체인을 따라가며 t_id, p_id 일치 시 해당 buf_descriptor return
    while (ht_entry) {
      if (ht_entry->buf_desc && 
          ht_entry->buf_desc->table_id == table_id &&
          ht_entry->buf_desc->page_num == page_num) {
        std::cout << "Found hashtable entry: " << ht_entry->buf_desc << std::endl;
        return ht_entry->buf_desc;
      }
      ht_entry = ht_entry->next; // 체이닝된 다음 노드로 이동
    }
//  ----------------------------------------------------------------------------
    std::cerr << "Hashtable entry not found for table_id: " << table_id << ", page_num: " << page_num << std::endl;

    return nullptr; // 일치하는 항목이 없으면 nullptr 반환
}

/**
 * @brief Insert a new buffer(page) into the hashtable.
 * 
 * @details Assume this page is not in the hashtable.
 */
inline void hashtable_insert(buf_descriptor_t *buf_desc) {
    if (buf_desc == nullptr) {
        std::cerr << "Error: buf_desc is nullptr in hashtable_insert." << std::endl;
        return;
    }

    // t_id, p_id에 해당하는 ht_entry를 계산
    ht_entry_t *ht_entry = get_ht_entry(buf_desc->table_id, buf_desc->page_num);

    // Null 체크 추가
    if (ht_entry == nullptr) {
        std::cerr << "Error: Calculated ht_entry is null. Hash function might be out of bounds." << std::endl;
        return;
    }

//  TODO -----------------------------------------------------------------------
    // ht_entry가 비어 있다면, 바로 할당하여 첫 번째 노드로 사용
    if (ht_entry->buf_desc == nullptr) {
        ht_entry->buf_desc = buf_desc;
        std::cout << "Inserted buf_desc directly at ht_entry: " << ht_entry 
                  << " for table_id: " << buf_desc->table_id 
                  << ", page_num: " << buf_desc->page_num << std::endl;
    } 
    else {
        // 해당 ht_entry에 새로운 buf_desc 노드를 생성 후 체인에 연결
        ht_entry_t *new_entry = new ht_entry_t;
        if (new_entry == nullptr) {
            std::cerr << "Error: Failed to allocate memory for new hash table entry." << std::endl;
            return;
        }

        new_entry->buf_desc = buf_desc;
        new_entry->next = ht_entry->next;
        ht_entry->next = new_entry;

        std::cout << "Inserting new hash entry at " << ht_entry 
                  << " for page_num: " << buf_desc->page_num << std::endl;
        std::cout << "New hash entry buf_desc: " << new_entry->buf_desc 
                  << ", table_id: " << new_entry->buf_desc->table_id 
                  << ", page_num: " << new_entry->buf_desc->page_num << std::endl;
    }
//  ----------------------------------------------------------------------------
}

/**
 * @brief Delete the buffer(page) from the hashtable.
 * 
 * @details Assume this page is in the hashtable.
 */
inline void hashtable_delete(buf_descriptor_t *buf_desc) {
    ht_entry_t *ht_entry = get_ht_entry(buf_desc->table_id, buf_desc->page_num);
    ht_entry_t *prev_entry = nullptr; // 이전 노드를 추적하기 위한 포인터

//  TODO -----------------------------------------------------------------------
    // 해당 entry의 체인을 따라가며 삭제할 항목 탐색
    while (ht_entry) {
        if (ht_entry->buf_desc == buf_desc) { // 삭제할 항목 찾을 시
            if (prev_entry) { // 중간 또는 끝 노드인 경우
                prev_entry->next = ht_entry->next;
            } else { // 시작 노드인 경우
                ht_entry->buf_desc = nullptr;
            }
            delete ht_entry;
            std::cout << "Deleted hashtable entry for page_num: " << buf_desc->page_num << std::endl;
            return;
        }
        // 노드를 한칸씩 이동
        prev_entry = ht_entry;
        ht_entry = ht_entry->next;
    }
//  ----------------------------------------------------------------------------
std::cerr << "Error: Entry not found in hashtable_delete for page_num: " << buf_desc->page_num << std::endl;
}

/**
 * @brief Get the victim buffer of eviction.
 * 
 * @return The memory address of the victim buffer.
 * 
 * @details
 * This function selects a victim buffer for eviction, following the buffer
 * replacement policy. (clock sweep algorithm in this code)
 * 
 * This first checks if there is an unused buffer in the buffer pool's freelist
 * and returns it if there is. Otherwise, selects and returns an appropriate
 * victim buffer according to the replacement policy.
 * 
 * Return NULL if all buffers in the buffer pool are pinned.
 */
buf_descriptor_t *get_victim_buffer() {
    // freelist에서 우선적으로 사용 가능한 버퍼가 있는지 확인
    buf_descriptor_t *buf_desc = get_from_freelist();
    if (buf_desc) {
        std::cout << "Found free buffer in freelist: " << buf_desc << std::endl;
        return buf_desc;
    }

    std::cerr << "No free buffer in freelist, using clock sweep for eviction." << std::endl;

//  TODO -----------------------------------------------------------------------
    int pinned_count = 0; // 순회 시작 시 pinned_count 초기화 (모든 버퍼가 참조 중인지를 확인)

    for (int i = 0; i < buffer_pool.num_buf; i++) {
    // while(true) {
        std::cout << "Clock hand: " << buffer_pool.clock_hand << ", Num buffers: " << buffer_pool.num_buf << std::endl;
        buf_descriptor_t* candidate = &buffer_pool.buf_descriptors[buffer_pool.clock_hand];

        if (candidate == nullptr) {
            std::cerr << "Error: candidate buffer descriptor is nullptr." << std::endl;
            continue;
        }

        // 참조 중인 페이지는 pinned_count만 증가시키고 넘어감
        if (candidate->reference_count > 0) {
            pinned_count++;
        } else {
            // 참조 중이지 않은 경우 교체 대상 고려
            if (candidate->usage_count == 0) {
                buffer_pool.clock_hand = (buffer_pool.clock_hand + 1) % buffer_pool.num_buf;
                std::cout << "Evicting Candidate: " << candidate << std::endl;
                return candidate;  // 교체 대상 반환
            } else {
                // 사용 횟수를 줄여 다음 순회에서 교체될 가능성 증가
                candidate->usage_count--;
            }
        }

        // 다음 버퍼로 이동
        buffer_pool.clock_hand = (buffer_pool.clock_hand + 1) % buffer_pool.num_buf;
    }

    assert(false);

    // 한 바퀴 순회 후 모든 버퍼가 참조 중이라면, nullptr 반환
    if (pinned_count == buffer_pool.num_buf) {
        std::cerr << "Error: All buffers are pinned, no victim buffer available." << std::endl;
        return nullptr; 
    }
//  ----------------------------------------------------------------------------
}

/**
 * @brief Get the buffer of the requested page.
 * 
 * @return The memory address of the buffer descriptor.
 * 
 * @details
 * This function receives a request for a page with table_id and page_num, and
 * returns a buffer containing that page. This first checks if the requested
 * page exists in the buffer pool using a hashtable. If the page exists in the
 * buffer pool, it returns the buffer simply. Otherwise, the other buffer is
 * reserved through the following steps:
 * 
 * 1. Get a new buffer by calling get_victim_buffer().
 * 
 * 2. Flush the buffer by calling flush_buffer() if the buffer is dirty.
 * 
 * 3. Delete the buffer from the hashtable if necessary.
 * 
 * 4. Set the table_id and page_num of the buffer and read page to the buffer by
 * calling file_read_page();
 * 
 * 5. Insert the buffer to the hashtable.
 * 
 * During this process, the usage count must not exceed MAX_USAGE_COUNT, and
 * buf_desc must increment the reference count by calling pin_buffer() before
 * being returned.
 */
buf_descriptor_t *get_buffer(int64_t table_id, pagenum_t page_num) {
    buf_descriptor_t *buf_desc = hashtable_lookup(table_id, page_num);

    stat_get_buffer++;

//  TODO -----------------------------------------------------------------------
    // page가 이미 buffer에 존재하는 경우
    if (buf_desc != nullptr) {
        std::cout << "buffer에 페이지가 이미 존재" << std::endl;
        if (buf_desc->reference_count == 0) {
            pin_buffer(buf_desc); // 필요할 때만 pin
        }
        std::cout << "buf_desc의 ref_count" << buf_desc->reference_count << "buf_desc의 usage_count" << buf_desc->usage_count << std::endl;
        return buf_desc;
    }
    std::cout << "buffer에 존재 하지 않는 경우 교체 대상 페이지 확보" << std::endl;
    // page가 buffer에 존재하지 않는 경우 교체 대상 페이지 확보
    buf_desc = get_victim_buffer();
    if (buf_desc == nullptr) { // 교체할 페이지 X(모든 버퍼가 참조 중)
        std::cerr << "get_victim_buffer returned nullptr, unable to proceed." << std::endl;
        return nullptr;
    }

    // 페이지가 dirty 상태인 경우 flush
    if (buf_desc->is_dirty) {
        std::cout << "페이지가 dirty한가?" << std::endl;
        flush_buffer(buf_desc);
    }

    // 기존 page가 해시 테이블에 존재하면 삭제
    hashtable_delete(buf_desc);

    // descriptor를 새로운 page로 초기화 
    buf_desc->table_id = table_id;
    buf_desc->page_num = page_num;
    buf_desc->is_dirty = false;
    
    // descriptor를 pin하고 hash table에 추가
    pin_buffer(buf_desc);
    hashtable_insert(buf_desc);
//  ----------------------------------------------------------------------------

    return buf_desc;
}

buf_descriptor_t *get_buffer_of_new_page(int64_t table_id) {
    buf_descriptor_t *header_buf = get_buffer(table_id, 0);
    page_t *header_page = header_buf->buf_page;
    pagenum_t new_page_num = header_page->free_page_num;
    buf_descriptor_t *buf_desc;

    // Ger header page and free page number from it.

    // If there is any free page, get the page.
    if (new_page_num != -1) {
        buf_desc = get_buffer(table_id, new_page_num);
        header_page->free_page_num = buf_desc->buf_page->next_free_page_num;
    }
    // Or not, double the database.
    else {
        // Double the database.
        uint64_t num_of_pages = 2 * header_page->num_of_pages;
        page_t *tmp_page = (page_t*)malloc(PAGE_SIZE);

        // Write new free page number.
        tmp_page->next_free_page_num = -1;
        file_write_page(table_id, header_page->num_of_pages, tmp_page);

        // Write free pages.
        for (pagenum_t i = header_page->num_of_pages; i < num_of_pages - 2;) {
            tmp_page->next_free_page_num = i++;
            file_write_page(table_id, i, tmp_page);
        }

        // Set header page.
        header_page->free_page_num = num_of_pages - 2;
        header_page->num_of_pages = num_of_pages;

        free(tmp_page);

        // Get new buffer.
        new_page_num = num_of_pages - 1;
        buf_desc = get_buffer(table_id, new_page_num);
    }

    mark_buffer_dirty(header_buf);
    unpin_buffer(header_buf);
    
    return buf_desc;
}

void free_page(int64_t table_id, buf_descriptor_t *buf) {
    buf_descriptor_t *header_buf = get_buffer(table_id, 0);
    page_t *header_page = header_buf->buf_page;
    page_t *buf_page = buf->buf_page;

    // Add this page to the free page list.
    buf_page->next_free_page_num = header_page->free_page_num;
    header_page->free_page_num = buf->page_num;

    mark_buffer_dirty(header_buf);
    mark_buffer_dirty(buf);
    unpin_buffer(header_buf);
}

int close_buffer_pool() {
//  TODO -----------------------------------------------------------------------
// dirty 상태의 buffer descriptor들을 flush 해주어야 함
    for (uint32_t i=0; i < buffer_pool.num_buf; i++) {
        buf_descriptor_t *buf_desc = &buffer_pool.buf_descriptors[i];
        if (buf_desc->is_dirty) {
            flush_buffer(buf_desc);
        }
    }

    delete[] buffer_pool.hashtable.ht_entries;
    delete[] buffer_pool.buf_descriptors;
    delete[] buffer_pool.buf_pages;
//  ----------------------------------------------------------------------------

    file_close_table_files();

    return 0;
}

void init_buffer_stat() {
    stat_get_buffer = 0;
    stat_read_page = 0;
    stat_write_page = 0;
}

int64_t get_buffer_hit_ratio() {
    return (stat_get_buffer - stat_read_page) * 100 /
           (stat_get_buffer);
}

std::string get_buffer_stat() {
    int64_t hit_ratio = get_buffer_hit_ratio();

    return string_format("get_buffer() count: %ld, file_read_page() count: %ld, file_write_page() count: %ld, buffer hit ratio: %ld%%",
                          stat_get_buffer, stat_read_page, stat_write_page, hit_ratio);
}

void print_buffer_stat() {
    std::cerr << get_buffer_stat() << std::endl;
}
