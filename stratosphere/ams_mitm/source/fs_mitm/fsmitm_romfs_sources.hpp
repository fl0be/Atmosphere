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

namespace ams::mitm::fs::romfs {

    class MetadataFile;

    enum class DataSourceType : u8 {
        Storage,
        File,
        LooseSdFile,
        Metadata,
        Memory,
    };

    struct LooseDirectory {
        u32 path_len;
        char path[];

        LooseDirectory(const char *dir_path, size_t dir_len) : path_len(static_cast<u32>(dir_len)) {
            std::memcpy(this->path, dir_path, dir_len);
        }
    };

    struct LoosePath {
        const LooseDirectory *directory;
        char name[];

        LoosePath(const LooseDirectory *dir, const char *file_name, size_t name_len) : directory(dir) {
            std::memcpy(this->name, file_name, name_len);
            this->name[name_len] = '\x00';
        }

        void GetFullPath(char *dst) const;
    };

    class LoosePathBlocks {
        NON_COPYABLE(LoosePathBlocks);
        NON_MOVEABLE(LoosePathBlocks);
        private:
            static constexpr size_t FirstBlockSize = 256;
            static constexpr size_t MaxBlockSize   = 4_KB;

            struct Block {
                Block *next;
                size_t size;
            };
        private:
            Block *m_blocks;
            u8 *m_cursor;
            size_t m_left;
        private:
            void AddBlock(size_t size);
            void *Allocate(size_t size);

            template<typename T, typename... Args>
            T *Create(size_t trailing, Args... args) {
                return std::construct_at(static_cast<T *>(this->Allocate(sizeof(T) + trailing)), args...);
            }
        public:
            LoosePathBlocks();
            ~LoosePathBlocks();

            const LooseDirectory *AddDirectory(const char *path, size_t path_len) {
                return this->Create<LooseDirectory>(path_len, path, path_len);
            }

            LoosePath *AddFile(const LooseDirectory *directory, const char *name, size_t name_len) {
                return this->Create<LoosePath>(name_len + 1, directory, name, name_len);
            }
    };

    struct SourceInfo {
        s64 virtual_offset;
        s64 size;
        union {
            struct {
                s64 offset;
            } storage_source_info;
            struct {
                s64 offset;
            } file_source_info;
            struct {
                LoosePath *path;
            } loose_source_info;
            struct {
                MetadataFile *file;
            } metadata_source_info;
            struct {
                u8 *data;
            } memory_source_info;
        };
        DataSourceType source_type;

        SourceInfo(s64 v_o, s64 sz, DataSourceType type, s64 p_o) : virtual_offset(v_o), size(sz), source_type(type) {
            switch (this->source_type) {
                case DataSourceType::Storage:
                    this->storage_source_info.offset = p_o;
                    break;
                case DataSourceType::File:
                    this->file_source_info.offset = p_o;
                    break;
                AMS_UNREACHABLE_DEFAULT_CASE();
            }
        }

        SourceInfo(s64 v_o, s64 sz, DataSourceType type, void *arg) : virtual_offset(v_o), size(sz), source_type(type) {
            switch (this->source_type) {
                case DataSourceType::LooseSdFile:
                    this->loose_source_info.path = static_cast<LoosePath *>(arg);
                    break;
                case DataSourceType::Metadata:
                    this->metadata_source_info.file = static_cast<MetadataFile *>(arg);
                    break;
                case DataSourceType::Memory:
                    this->memory_source_info.data = static_cast<u8 *>(arg);
                    break;
                AMS_UNREACHABLE_DEFAULT_CASE();
            }
        }
    };

    constexpr inline bool operator<(const s64 lhs, const SourceInfo &rhs) {
        return lhs < rhs.virtual_offset;
    }

    constexpr inline bool operator<(const SourceInfo &lhs, const s64 rhs) {
        return lhs.virtual_offset < rhs;
    }

    class SourceInfoChunks {
        NON_COPYABLE(SourceInfoChunks);
        NON_MOVEABLE(SourceInfoChunks);
        private:
            static constexpr size_t ChunkSize       = 4_KB;
            static constexpr size_t SourcesPerChunk = ChunkSize / sizeof(SourceInfo);

            struct Chunk {
                SourceInfo *sources;
                s64 virtual_offset;

                friend bool operator<(s64 offset, const Chunk &chunk) { return offset < chunk.virtual_offset; }
                friend bool operator<(const Chunk &chunk, s64 offset) { return chunk.virtual_offset < offset; }
            };
        private:
            Chunk *m_chunks;
            size_t m_index_size;
            size_t m_count;
            LoosePathBlocks m_loose_path_blocks;
        private:
            SourceInfo *GetSource(size_t i) const {
                const size_t chunk = i / SourcesPerChunk;
                const size_t slot  = i % SourcesPerChunk;
                return std::addressof(m_chunks[chunk].sources[slot]);
            }

            SourceInfo *GetBack() const {
                AMS_ABORT_UNLESS(m_count > 0);
                return this->GetSource(m_count - 1);
            }

            size_t GetChunkCount() const;

            void AllocateChunk(s64 virtual_offset);
        public:
            SourceInfoChunks();
            ~SourceInfoChunks();

            const SourceInfo &operator[](size_t i) const {
                AMS_ABORT_UNLESS(i < m_count);
                return *this->GetSource(i);
            }

            const SourceInfo &Back() const { return *this->GetBack(); }

            void Initialize(size_t count);

            template<typename Arg>
            ALWAYS_INLINE void EmplaceBack(s64 virtual_offset, s64 size, DataSourceType type, Arg arg) {
                if (m_count % SourcesPerChunk == 0) {
                    this->AllocateChunk(virtual_offset);
                }
                std::construct_at(this->GetSource(m_count), virtual_offset, size, type, arg);
                ++m_count;
            }

            LoosePathBlocks &GetLoosePathBlocks() { return m_loose_path_blocks; }

            ALWAYS_INLINE void ExtendBackTo(s64 end_offset) {
                SourceInfo * const back = this->GetBack();
                back->size = end_offset - back->virtual_offset;
            }

            size_t GetIndexAt(s64 offset) const;
    };

}
