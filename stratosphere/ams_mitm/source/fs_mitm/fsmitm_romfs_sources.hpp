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
                char *path;
            } loose_source_info;
            struct {
                MetadataFile *file;
            } metadata_source_info;
            struct {
                u8 *data;
            } memory_source_info;
        };
        DataSourceType source_type;
        bool cleaned_up;

        SourceInfo(s64 v_o, s64 sz, DataSourceType type, s64 p_o) : virtual_offset(v_o), size(sz), source_type(type), cleaned_up(false) {
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

        SourceInfo(s64 v_o, s64 sz, DataSourceType type, void *arg) : virtual_offset(v_o), size(sz), source_type(type), cleaned_up(false) {
            switch (this->source_type) {
                case DataSourceType::LooseSdFile:
                    this->loose_source_info.path = static_cast<char *>(arg);
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

        void Cleanup() {
            AMS_ABORT_UNLESS(!this->cleaned_up);
            this->cleaned_up = true;

            switch (this->source_type) {
                case DataSourceType::Storage:
                case DataSourceType::File:
                case DataSourceType::Metadata:
                case DataSourceType::Memory:
                    break;
                case DataSourceType::LooseSdFile:
                    FreeTracked(AllocationType_FullPath, this->loose_source_info.path, std::strlen(this->loose_source_info.path) + 1);
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

            ALWAYS_INLINE void ExtendBackTo(s64 end_offset) {
                SourceInfo * const back = this->GetBack();
                back->size = end_offset - back->virtual_offset;
            }

            size_t GetIndexAt(s64 offset) const;
    };

}
