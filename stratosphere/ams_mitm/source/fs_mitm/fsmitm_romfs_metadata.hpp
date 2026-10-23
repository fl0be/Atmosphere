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
#include "fsmitm_romfs_tables.hpp"

namespace ams::mitm::fs::romfs {

    struct MetadataLayout {
        size_t dir_bucket_size;
        size_t dir_entry_size;
        size_t file_bucket_size;
        size_t file_entry_size;

        constexpr size_t DirBucketOffset()  const { return 0; }
        constexpr size_t DirEntryOffset()   const { return this->DirBucketOffset()  + dir_bucket_size; }
        constexpr size_t FileBucketOffset() const { return this->DirEntryOffset()   + dir_entry_size; }
        constexpr size_t FileEntryOffset()  const { return this->FileBucketOffset() + file_bucket_size; }
        constexpr size_t TotalSize()        const { return this->FileEntryOffset()  + file_entry_size; }
    };

    class MetadataFile {
        NON_COPYABLE(MetadataFile);
        NON_MOVEABLE(MetadataFile);
        private:
            static constexpr const char FileName[] = "romfs_metadata.bin";
        private:
            ::FsFile m_file;
            MetadataLayout m_layout;
            size_t m_dir_buckets_written;
            size_t m_file_buckets_written;
            bool m_owns_file;
            util::optional<DirectoryTableWriter> m_dir_table;
            util::optional<FileTableWriter> m_file_table;
        public:
            MetadataFile();
            ~MetadataFile();

            void Create(ncm::ProgramId program_id, const MetadataLayout &layout);

            DirectoryEntry *AppendDirectoryEntry(u32 name_len);
            void FinishDirectoryTable();

            FileEntry *AppendFileEntry(u32 name_len);
            void FinishFileTable();

            void AppendDirectoryBuckets(const void *src, size_t size);
            void AppendFileBuckets(const void *src, size_t size);

            void Flush();

            ams::fs::RemoteFile *ReleaseAsRemoteFile();
    };

}
