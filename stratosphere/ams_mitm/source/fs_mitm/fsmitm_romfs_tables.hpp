/*
 * Copyright (c) Atmosphère-NX
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms and conditions of the GNU General Public License,
 * version 2, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stratosphere.hpp>
#include "fsmitm_romfs_heap.hpp"
#include "fsmitm_romfs_format.hpp"

namespace ams::mitm::fs::romfs {

    class DynamicTableCache {
        NON_COPYABLE(DynamicTableCache);
        NON_MOVEABLE(DynamicTableCache);
        private:
            static constexpr size_t MaxCachedSize = (1_MB / 4);
        private:
            size_t m_cache_bitsize;
            size_t m_cache_size;
        protected:
            void *m_cache;
        protected:
            DynamicTableCache(size_t sz) {
                m_cache_size = util::CeilingPowerOfTwo(std::min(sz, MaxCachedSize));
                m_cache = AllocateTracked(AllocationType_TableCache, m_cache_size, false);
                while (m_cache == nullptr) {
                    m_cache_size  >>= 1;
                    AMS_ABORT_UNLESS(m_cache_size  >= 16_KB);
                    m_cache = AllocateTracked(AllocationType_TableCache, m_cache_size, false);
                }
                m_cache_bitsize = util::CountTrailingZeros(m_cache_size);
            }

            ~DynamicTableCache() {
                FreeTracked(AllocationType_TableCache, m_cache, m_cache_size);
            }

            ALWAYS_INLINE size_t GetCacheSize() const { return static_cast<size_t>(1) << m_cache_bitsize; }
    };

    class HashTableStorage : public DynamicTableCache {
        public:
            HashTableStorage(size_t sz) : DynamicTableCache(sz) { /* ... */ }

            ALWAYS_INLINE u32 *GetBuffer() { return reinterpret_cast<u32 *>(m_cache); }
            ALWAYS_INLINE size_t GetBufferSize() const { return DynamicTableCache::GetCacheSize(); }
    };

    template<typename Entry>
    class TableReader : public DynamicTableCache {
        NON_COPYABLE(TableReader);
        NON_MOVEABLE(TableReader);
        private:
            static constexpr size_t FallbackCacheSize = 1_KB;
        private:
            ams::fs::IStorage *m_storage;
            size_t m_offset;
            size_t m_size;
            size_t m_cache_idx;
            u8 m_fallback_cache[FallbackCacheSize];
        private:
            ALWAYS_INLINE bool Read(size_t ofs, void *dst, size_t size) {
                R_TRY_CATCH(m_storage->Read(m_offset + ofs, dst, size)) {
                    R_CATCH(ams::fs::ResultNcaExternalKeyNotFound) { return false; }
                } R_END_TRY_CATCH_WITH_ABORT_UNLESS;

                return true;
            }
            ALWAYS_INLINE bool ReloadCacheImpl(size_t idx) {
                const size_t rel_ofs = idx * this->GetCacheSize();
                AMS_ABORT_UNLESS(rel_ofs < m_size);
                const size_t new_cache_size = std::min(m_size - rel_ofs, this->GetCacheSize());
                if (!this->Read(rel_ofs, m_cache, new_cache_size)) {
                    return false;
                }

                m_cache_idx = idx;
                return true;
            }

            ALWAYS_INLINE bool ReloadCache(size_t idx) {
                if (m_cache_idx != idx) {
                    if (!this->ReloadCacheImpl(idx)) {
                        return false;
                    }
                }

                return true;
            }

            ALWAYS_INLINE size_t GetCacheIndex(u32 ofs) {
                return ofs / this->GetCacheSize();
            }
        public:
            TableReader(ams::fs::IStorage *s, size_t ofs, size_t sz) : DynamicTableCache(sz), m_storage(s), m_offset(ofs), m_size(sz), m_cache_idx(0) {
                AMS_ABORT_UNLESS(m_cache != nullptr);
                this->ReloadCacheImpl(0);
            }

            const Entry *GetEntry(u32 entry_offset) {
                if (!this->ReloadCache(this->GetCacheIndex(entry_offset))) {
                    return nullptr;
                }

                const size_t ofs = entry_offset % this->GetCacheSize();

                const Entry *entry = reinterpret_cast<const Entry *>(reinterpret_cast<uintptr_t>(m_cache) + ofs);
                if (AMS_UNLIKELY(this->GetCacheIndex(entry_offset) != this->GetCacheIndex(entry_offset + sizeof(Entry) + entry->name_size + sizeof(u32)))) {
                    if (!this->Read(entry_offset, m_fallback_cache, std::min(m_size - entry_offset, FallbackCacheSize))) {
                        return nullptr;
                    }

                    entry = reinterpret_cast<const Entry *>(m_fallback_cache);
                }
                return entry;
            }
    };
    
    template<typename Entry>
    class TableWriter : public DynamicTableCache {
        NON_COPYABLE(TableWriter);
        NON_MOVEABLE(TableWriter);
        private:
            ::FsFile *m_file;
            size_t m_offset;
            size_t m_size;
            size_t m_written;
            size_t m_held;
        private:
            ALWAYS_INLINE void Flush() {
                if (m_held > 0) {
                    R_ABORT_UNLESS(fsFileWrite(m_file, m_offset + m_written, m_cache, m_held, FsWriteOption_None));
                    m_written += m_held;
                    m_held     = 0;
                }
            }
        public:
            TableWriter(::FsFile *f, size_t ofs, size_t sz) : DynamicTableCache(sz), m_file(f), m_offset(ofs), m_size(sz), m_written(0), m_held(0) { /* ... */ }

            ~TableWriter() {
                this->Flush();

                AMS_ABORT_UNLESS(m_written == m_size);
            }

            Entry *AppendEntry(u32 name_len) {
                const size_t entry_size = EntrySize<Entry>(name_len);

                AMS_ABORT_UNLESS(m_written + m_held + entry_size <= m_size);

                if (m_held + entry_size > this->GetCacheSize()) {
                    this->Flush();
                }

                Entry * const entry = reinterpret_cast<Entry *>(static_cast<u8 *>(m_cache) + m_held);
                m_held += entry_size;
                return entry;
            }
    };

    using DirectoryTableWriter = TableWriter<DirectoryEntry>;
    using FileTableWriter      = TableWriter<FileEntry>;

    class DirectoryTableReader : public TableReader<DirectoryEntry> {
        public:
            DirectoryTableReader(ams::fs::IStorage *s, size_t ofs, size_t sz) : TableReader(s, ofs, sz) { /* ... */ }
    };
    
    class FileTableReader : public TableReader<FileEntry> {
        public:
            FileTableReader(ams::fs::IStorage *s, size_t ofs, size_t sz) : TableReader(s, ofs, sz) { /* ... */ }
    };

}
