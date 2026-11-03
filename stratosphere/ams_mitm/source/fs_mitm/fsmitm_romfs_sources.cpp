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
#include <stratosphere.hpp>
#include "fsmitm_romfs_sources.hpp"

namespace ams::mitm::fs {

    using namespace ams::fs;

    namespace romfs {

        namespace {

            template<typename T>
            size_t GetUpperBound(const T *entries, size_t count, s64 offset) {
                return std::upper_bound(entries, entries + count, offset) - entries;
            }

        }

        size_t SourceInfoChunks::GetChunkCount() const {
            return util::DivideUp(m_count, SourcesPerChunk);
        }

        void SourceInfoChunks::AllocateChunk(s64 virtual_offset) {
            const size_t i = m_count / SourcesPerChunk;
            AMS_ABORT_UNLESS(i < m_index_size);

            Chunk &chunk = m_chunks[i];
            chunk.sources        = AllocateTracked<SourceInfo>(AllocationType_SourceInfo, ChunkSize);
            chunk.virtual_offset = virtual_offset;
        }

        SourceInfoChunks::SourceInfoChunks() : m_chunks(nullptr), m_index_size(0), m_count(0) { /* ... */ }

        SourceInfoChunks::~SourceInfoChunks() {
            for (size_t i = 0; i < m_count; ++i) {
                this->GetSource(i)->Cleanup();
            }
            for (size_t i = 0; i < this->GetChunkCount(); ++i) {
                FreeTracked(AllocationType_SourceInfo, m_chunks[i].sources, ChunkSize);
            }
            if (m_chunks != nullptr) {
                FreeTracked(AllocationType_SourceInfo, m_chunks, sizeof(Chunk) * m_index_size);
            }
        }

        void SourceInfoChunks::Initialize(size_t count) {
            AMS_ABORT_UNLESS(m_chunks == nullptr);

            m_index_size = util::DivideUp(count, SourcesPerChunk);
            m_chunks = AllocateTracked<Chunk>(AllocationType_SourceInfo, sizeof(Chunk) * m_index_size);
        }

        size_t SourceInfoChunks::GetIndexAt(s64 offset) const {
            AMS_ABORT_UNLESS(m_count > 0 && offset >= m_chunks[0].virtual_offset);

            const size_t chunk = GetUpperBound(m_chunks, this->GetChunkCount(), offset) - 1;

            const size_t first = chunk * SourcesPerChunk;
            const size_t slot  = GetUpperBound(m_chunks[chunk].sources, std::min(SourcesPerChunk, m_count - first), offset) - 1;

            return first + slot;
        }

    }

}
