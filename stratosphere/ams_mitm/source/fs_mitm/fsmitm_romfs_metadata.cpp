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
#include "../amsmitm_fs_utils.hpp"
#include "fsmitm_romfs_metadata.hpp"

namespace ams::mitm::fs {

    using namespace ams::fs;

    namespace romfs {

        MetadataFile::MetadataFile(ncm::ProgramId program_id) : m_program_id(program_id), m_file{}, m_opened(false) { /* ... */ }

        MetadataFile::~MetadataFile() {
            this->Close();
        }

        void MetadataFile::Create(s64 size) {
            AMS_ABORT_UNLESS(!m_opened);

            R_ABORT_UNLESS(mitm::fs::CreateAndOpenAtmosphereSdFile(std::addressof(m_file), m_program_id, FileName, size));

            m_opened = true;
        }

        void MetadataFile::Close() {
            if (m_opened) {
                fsFileClose(std::addressof(m_file));
                m_opened = false;
            }
        }

        void MetadataFile::Flush() {
            R_ABORT_UNLESS(fsFileFlush(std::addressof(m_file)));
        }

        void MetadataFile::Read(s64 offset, void *dst, size_t size) {
            u64 read = 0;
            R_ABORT_UNLESS(fsFileRead(std::addressof(m_file), offset, dst, size, FsReadOption_None, std::addressof(read)));
            AMS_ABORT_UNLESS(read == size);
        }

        MetadataWriter::MetadataWriter(MetadataFile *file) : m_file(file), m_layout{}, m_dir_buckets_written(0), m_file_buckets_written(0) { /* ... */ }

        void MetadataWriter::Create(const MetadataLayout &layout) {
            m_file->Create(layout.TotalSize());
            m_layout = layout;
        }

        DirectoryEntry *MetadataWriter::AppendDirectoryEntry(u32 name_len) {
            if (!m_dir_table.has_value()) {
                m_dir_table.emplace(std::addressof(m_file->m_file), m_layout.DirEntryOffset(), m_layout.dir_entry_size);
            }
            return m_dir_table->AppendEntry(name_len);
        }

        void MetadataWriter::FinishDirectoryTable() {
            AMS_ABORT_UNLESS(m_dir_table.has_value() || m_layout.dir_entry_size == 0);

            m_dir_table.reset();
        }

        FileEntry *MetadataWriter::AppendFileEntry(u32 name_len) {
            if (!m_file_table.has_value()) {
                m_file_table.emplace(std::addressof(m_file->m_file), m_layout.FileEntryOffset(), m_layout.file_entry_size);
            }
            return m_file_table->AppendEntry(name_len);
        }

        void MetadataWriter::FinishFileTable() {
            AMS_ABORT_UNLESS(m_file_table.has_value() || m_layout.file_entry_size == 0);

            m_file_table.reset();
        }

        void MetadataWriter::AppendDirectoryBuckets(const void *src, size_t size) {
            AMS_ABORT_UNLESS(m_dir_buckets_written + size <= m_layout.dir_bucket_size);

            R_ABORT_UNLESS(fsFileWrite(std::addressof(m_file->m_file), m_layout.DirBucketOffset() + m_dir_buckets_written, src, size, FsWriteOption_None));
            m_dir_buckets_written += size;
        }

        void MetadataWriter::AppendFileBuckets(const void *src, size_t size) {
            AMS_ABORT_UNLESS(m_file_buckets_written + size <= m_layout.file_bucket_size);

            R_ABORT_UNLESS(fsFileWrite(std::addressof(m_file->m_file), m_layout.FileBucketOffset() + m_file_buckets_written, src, size, FsWriteOption_None));
            m_file_buckets_written += size;
        }

        void MetadataWriter::Finish() {
            AMS_ABORT_UNLESS(m_file->IsOpen());
            AMS_ABORT_UNLESS(!m_dir_table.has_value() && !m_file_table.has_value());
            AMS_ABORT_UNLESS(m_dir_buckets_written == m_layout.dir_bucket_size && m_file_buckets_written == m_layout.file_bucket_size);

            m_file->Flush();
        }

    }

}
