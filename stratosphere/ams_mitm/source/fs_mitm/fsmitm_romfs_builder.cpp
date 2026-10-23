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
#include "fsmitm_romfs_builder.hpp"
#include "fsmitm_romfs_format.hpp"
#include "fsmitm_romfs_tables.hpp"

namespace ams::mitm::fs {

    using namespace ams::fs;

    namespace romfs {

        Builder::Builder(MergedTree *tree) : m_tree(tree), m_layout{}, m_file_partition_size(0) { /* ... */ }

        void Builder::Build(SourceInfoVector *out_infos) {
            /* Clear output. */
            out_infos->clear();

            /* Open an SD card filesystem. */
            FsFileSystem sd_filesystem;
            R_ABORT_UNLESS(fsOpenSdCardFileSystem(std::addressof(sd_filesystem)));
            ON_SCOPE_EXIT { fsFsClose(std::addressof(sd_filesystem)); };

            /* Calculate hash table sizes. */
            const size_t num_dir_hash_table_entries  = GetHashTableSize(m_tree->m_num_dirs);
            const size_t num_file_hash_table_entries = GetHashTableSize(m_tree->m_num_files);
            m_layout.dir_bucket_size  = sizeof(u32) * num_dir_hash_table_entries;
            m_layout.dir_entry_size   = m_tree->m_dir_table_size;
            m_layout.file_bucket_size = sizeof(u32) * num_file_hash_table_entries;
            m_layout.file_entry_size  = m_tree->m_file_table_size;

            /* Allocate metadata, make pointers. */
            RomfsHeader *header = AllocateTracked<RomfsHeader>(AllocationType_Memory, sizeof(RomfsHeader));
            std::memset(header, 0x00, sizeof(*header));

            /* Open metadata file. */
            MetadataFile metadata;
            metadata.Create(m_tree->m_program_id, m_layout);

            /* Ensure later hash tables will have correct defaults. */
            static_assert(EmptyEntry == 0xFFFFFFFF);

            /* Emplace metadata source info. */
            out_infos->emplace_back(0, sizeof(*header), DataSourceType::Memory, header);

            /* Process Files. */
            {
                u32 entry_offset = 0;
                BuildFileContext *cur_file = nullptr;
                BuildFileContext *prev_file = nullptr;
                for (const auto &it : m_tree->m_files) {
                    cur_file = it.get();

                    /* By default, pad to 0x10 alignment. */
                    m_file_partition_size = AlignFileContent(m_file_partition_size);

                    /* Check if extra padding is present in original source, preserve it to make our life easier. */
                    const bool is_storage_or_file = cur_file->source_type == DataSourceType::Storage || cur_file->source_type == DataSourceType::File;
                    if (prev_file != nullptr && prev_file->source_type == cur_file->source_type && is_storage_or_file) {
                        const s64 expected = m_file_partition_size - prev_file->offset + prev_file->orig_offset;
                        if (expected != cur_file->orig_offset) {
                            AMS_ABORT_UNLESS(expected <= cur_file->orig_offset);
                            m_file_partition_size += cur_file->orig_offset - expected;
                        }
                    }

                    /* Calculate offsets. */
                    cur_file->offset = m_file_partition_size;
                    m_file_partition_size += cur_file->size;
                    cur_file->entry_offset = entry_offset;
                    entry_offset += EntrySize<FileEntry>(cur_file->path_len);

                    /* Save current file as prev for next iteration. */
                    prev_file = cur_file;
                }
                /* Assign deferred parent/sibling ownership. */
                for (auto it = m_tree->m_files.rbegin(); it != m_tree->m_files.rend(); it++) {
                    cur_file = it->get();
                    cur_file->sibling = cur_file->parent->file;
                    cur_file->parent->file = cur_file;
                }
            }

            /* Process Directories. */
            {
                u32 entry_offset = 0;
                BuildDirectoryContext *cur_dir = nullptr;
                for (const auto &it : m_tree->m_directories) {
                    cur_dir = it.get();
                    cur_dir->entry_offset = entry_offset;
                    entry_offset += EntrySize<DirectoryEntry>(cur_dir->path_len);
                }
                /* Assign deferred parent/sibling ownership. */
                for (auto it = m_tree->m_directories.rbegin(); it != m_tree->m_directories.rend(); it++) {
                    cur_dir = it->get();
                    if (cur_dir == m_tree->m_root) {
                        continue;
                    }
                    cur_dir->sibling = cur_dir->parent->child;
                    cur_dir->parent->child = cur_dir;
                }
            }

            /* Set all files' hash value = hash index. */
            for (const auto &it : m_tree->m_files) {
                BuildFileContext *cur_file = it.get();
                cur_file->hash_value = GetBucketIndex(cur_file->parent->entry_offset, cur_file->path, cur_file->path_len, num_file_hash_table_entries);
            }

            /* Set all directories' hash value = hash index. */
            for (const auto &it : m_tree->m_directories) {
                BuildDirectoryContext *cur_dir = it.get();
                cur_dir->hash_value = GetBucketIndex(cur_dir == m_tree->m_root ? RootEntryOffset : cur_dir->parent->entry_offset, cur_dir->path, cur_dir->path_len, num_dir_hash_table_entries);
            }

            /* Write hash tables. */
            {
                HashTableStorage hash_table_storage(std::max(m_layout.dir_bucket_size, m_layout.file_bucket_size));

                u32 *hash_table = hash_table_storage.GetBuffer();
                size_t hash_table_size = hash_table_storage.GetBufferSize();

                /* Write the file hash table. */
                for (size_t ofs = 0; ofs < m_layout.file_bucket_size; ofs += hash_table_size) {
                    std::memset(hash_table, 0xFF, hash_table_size);

                    const u32 ofs_ind = ofs / sizeof(u32);
                    const u32 end_ind = (ofs + hash_table_size) / sizeof(u32);

                    for (const auto &it : m_tree->m_files) {
                        BuildFileContext *cur_file = it.get();
                        if (cur_file->HasHashMark()) {
                            continue;
                        }

                        if (const auto hash_ind = cur_file->hash_value; ofs_ind <= hash_ind && hash_ind < end_ind) {
                            cur_file->hash_value = hash_table[hash_ind - ofs_ind];
                            hash_table[hash_ind - ofs_ind] = cur_file->entry_offset;

                            cur_file->SetHashMark();
                        }
                    }

                    metadata.AppendFileBuckets(hash_table, std::min(m_layout.file_bucket_size - ofs, hash_table_size));
                }

                /* Write the directory hash table. */
                for (size_t ofs = 0; ofs < m_layout.dir_bucket_size; ofs += hash_table_size) {
                    std::memset(hash_table, 0xFF, hash_table_size);

                    const u32 ofs_ind = ofs / sizeof(u32);
                    const u32 end_ind = (ofs + hash_table_size) / sizeof(u32);

                    for (const auto &it : m_tree->m_directories) {
                        BuildDirectoryContext *cur_dir = it.get();
                        if (cur_dir->HasHashMark()) {
                            continue;
                        }

                        if (const auto hash_ind = cur_dir->hash_value; ofs_ind <= hash_ind && hash_ind < end_ind) {
                            cur_dir->hash_value = hash_table[hash_ind - ofs_ind];
                            hash_table[hash_ind - ofs_ind] = cur_dir->entry_offset;

                            cur_dir->SetHashMark();
                        }
                    }

                    metadata.AppendDirectoryBuckets(hash_table, std::min(m_layout.dir_bucket_size - ofs, hash_table_size));
                }
            }

            /* Replace sibling pointers with sibling entry_offsets, so that we can de-allocate as we go. */
            {
                /* Set all directories sibling and file pointers. */
                for (const auto &it : m_tree->m_directories) {
                    BuildDirectoryContext *cur_dir = it.get();

                    cur_dir->ClearHashMark();

                    cur_dir->sibling_offset = (cur_dir->sibling == nullptr) ? EmptyEntry : cur_dir->sibling->entry_offset;
                    cur_dir->child_offset   = (cur_dir->child   == nullptr) ? EmptyEntry : cur_dir->child->entry_offset;
                    cur_dir->file_offset    = (cur_dir->file    == nullptr) ? EmptyEntry : cur_dir->file->entry_offset;

                    cur_dir->parent_offset  = cur_dir == m_tree->m_root ? RootEntryOffset : cur_dir->parent->entry_offset;
                }

                /* Replace all files' sibling pointers. */
                for (const auto &it : m_tree->m_files) {
                    BuildFileContext *cur_file = it.get();

                    cur_file->ClearHashMark();

                    cur_file->sibling_offset = (cur_file->sibling == nullptr) ? EmptyEntry : cur_file->sibling->entry_offset;
                }
            }

            /* Write the file table. */
            {
                for (auto it = m_tree->m_files.begin(); it != m_tree->m_files.end(); it = m_tree->m_files.erase(it)) {
                    BuildFileContext *cur_file = it->get();
                    FileEntry *cur_entry = metadata.AppendFileEntry(cur_file->path_len);

                    /* Set entry fields. */
                    cur_entry->parent  = cur_file->parent->entry_offset;
                    cur_entry->sibling = cur_file->sibling_offset;
                    cur_entry->offset  = cur_file->offset;
                    cur_entry->size    = cur_file->size;
                    cur_entry->hash    = cur_file->hash_value;

                    /* Set name. */
                    SetEntryName(cur_entry, cur_file->path, cur_file->path_len);

                    /* Emplace a source. */
                    switch (cur_file->source_type) {
                        case DataSourceType::Storage:
                        case DataSourceType::File:
                            {
                                /* Try to compact if possible. */
                                auto &back = out_infos->back();
                                if (back.source_type == cur_file->source_type) {
                                    back.size = cur_file->offset + FilePartitionOffset + cur_file->size - back.virtual_offset;
                                } else {
                                    out_infos->emplace_back(cur_file->offset + FilePartitionOffset, cur_file->size, cur_file->source_type, cur_file->orig_offset + FilePartitionOffset);
                                }
                            }
                            break;
                        case DataSourceType::LooseSdFile:
                            {
                                char full_path[fs::EntryNameLengthMax + 1];
                                const size_t path_needed_size = cur_file->GetPathLength() + 1;
                                AMS_ABORT_UNLESS(path_needed_size <= sizeof(full_path));
                                cur_file->GetPath(full_path);

                                FreeTracked(AllocationType_FileName, cur_file->path, cur_file->path_len + 1);
                                cur_file->path = nullptr;

                                char *new_path = AllocateTracked<char>(AllocationType_FullPath, path_needed_size);
                                std::memcpy(new_path, full_path, path_needed_size);
                                out_infos->emplace_back(cur_file->offset + FilePartitionOffset, cur_file->size, cur_file->source_type, new_path);
                            }
                            break;
                        AMS_UNREACHABLE_DEFAULT_CASE();
                    }
                }

                metadata.FinishFileTable();
            }

            /* Write the directory table. */
            {
                for (auto it = m_tree->m_directories.begin(); it != m_tree->m_directories.end(); it = m_tree->m_directories.erase(it)) {
                    BuildDirectoryContext *cur_dir = it->get();
                    DirectoryEntry *cur_entry = metadata.AppendDirectoryEntry(cur_dir->path_len);

                    /* Set entry fields. */
                    cur_entry->parent  = cur_dir->parent_offset;
                    cur_entry->sibling = cur_dir->sibling_offset;
                    cur_entry->child   = cur_dir->child_offset;
                    cur_entry->file    = cur_dir->file_offset;
                    cur_entry->hash    = cur_dir->hash_value;

                    /* Set name. */
                    SetEntryName(cur_entry, cur_dir->path, cur_dir->path_len);
                }

                metadata.FinishDirectoryTable();
            }

            /* Delete maps. */
            m_tree->m_root = nullptr;
            m_tree->m_directories.clear();
            m_tree->m_files.clear();

            /* Set header fields. */
            header->size                    = sizeof(*header);
            header->file_bucket_size        = m_layout.file_bucket_size;
            header->file_entry_size         = m_layout.file_entry_size;
            header->directory_bucket_size   = m_layout.dir_bucket_size;
            header->directory_entry_size    = m_layout.dir_entry_size;
            header->body_offset             = FilePartitionOffset;

            const s64 metadata_begin        = AlignMetadataOffset(FilePartitionOffset + m_file_partition_size);
            header->directory_bucket_offset = metadata_begin + m_layout.DirBucketOffset();
            header->directory_entry_offset  = metadata_begin + m_layout.DirEntryOffset();
            header->file_bucket_offset      = metadata_begin + m_layout.FileBucketOffset();
            header->file_entry_offset       = metadata_begin + m_layout.FileEntryOffset();

            /* Save metadata to the SD card, to save on memory space. */
            {
                metadata.Flush();
                out_infos->emplace_back(header->directory_bucket_offset, m_layout.TotalSize(), DataSourceType::Metadata, metadata.ReleaseAsRemoteFile());
            }
        }

    }

}
