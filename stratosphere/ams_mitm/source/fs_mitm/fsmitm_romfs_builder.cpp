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
#include "fsmitm_romfs_builder.hpp"
#include "fsmitm_romfs_format.hpp"
#include "fsmitm_romfs_metadata.hpp"
#include "fsmitm_romfs_tables.hpp"
#include "fsmitm_romfs_tree.hpp"

namespace ams::mitm::fs {

    using namespace ams::fs;

    namespace romfs {

        namespace {

            constinit os::SdkMutex g_fs_romfs_path_lock;
            constinit char g_fs_romfs_path_buffer[fs::EntryNameLengthMax + 1];

            NOINLINE void OpenFileSystemRomfsDirectory(FsDir *out, ncm::ProgramId program_id, BuildDirectoryContext *parent, fs::OpenDirectoryMode mode, FsFileSystem *fs) {
                std::scoped_lock lk(g_fs_romfs_path_lock);
                parent->GetPath(g_fs_romfs_path_buffer);
                R_ABORT_UNLESS(mitm::fs::OpenAtmosphereRomfsDirectory(out, program_id, g_fs_romfs_path_buffer, mode, fs));
            }

            void VisitDirectory(MergedTree *tree, ncm::ProgramId program_id, ::FsDirectoryEntry *dir_entry, FsFileSystem *fs, BuildDirectoryContext *parent) {
                FsDir dir;

                /* Get number of child directories. */
                s64 num_child_dirs = 0;
                {
                    OpenFileSystemRomfsDirectory(std::addressof(dir), program_id, parent, OpenDirectoryMode_Directory, fs);
                    ON_SCOPE_EXIT { fsDirClose(std::addressof(dir)); };
                    R_ABORT_UNLESS(fsDirGetEntryCount(std::addressof(dir), std::addressof(num_child_dirs)));
                }
                AMS_ABORT_UNLESS(num_child_dirs >= 0);

                {
                    BuildDirectoryContext **child_dirs = num_child_dirs != 0 ? AllocateTracked<BuildDirectoryContext *>(AllocationType_DirPointerArray, sizeof(BuildDirectoryContext *) * num_child_dirs) : nullptr;
                    AMS_ABORT_UNLESS(num_child_dirs == 0 || child_dirs != nullptr);
                    ON_SCOPE_EXIT { if (child_dirs != nullptr) { FreeTracked(AllocationType_DirPointerArray, child_dirs, sizeof(BuildDirectoryContext *) * num_child_dirs); } };

                    s64 cur_child_dir_ind = 0;
                    {
                        OpenFileSystemRomfsDirectory(std::addressof(dir), program_id, parent, OpenDirectoryMode_All, fs);
                        ON_SCOPE_EXIT { fsDirClose(std::addressof(dir)); };

                        s64 read_entries = 0;
                        while (true) {
                            R_ABORT_UNLESS(fsDirRead(std::addressof(dir), std::addressof(read_entries), 1, dir_entry));
                            if (read_entries != 1) {
                                break;
                            }

                            AMS_ABORT_UNLESS(dir_entry->type == FsDirEntryType_Dir || dir_entry->type == FsDirEntryType_File);
                            if (dir_entry->type == FsDirEntryType_Dir) {
                                AMS_ABORT_UNLESS(child_dirs != nullptr);

                                BuildDirectoryContext *real_child = nullptr;
                                tree->AddDirectory(std::addressof(real_child), parent, std::unique_ptr<BuildDirectoryContext>(AllocateTyped<BuildDirectoryContext>(AllocationType_BuildDirContext, dir_entry->name, strlen(dir_entry->name))));
                                AMS_ABORT_UNLESS(real_child != nullptr);
                                child_dirs[cur_child_dir_ind++] = real_child;
                                AMS_ABORT_UNLESS(cur_child_dir_ind <= num_child_dirs);
                            } else /* if (dir_entry->type == FsDirEntryType_File) */ {
                                tree->AddFile(parent, std::unique_ptr<BuildFileContext>(AllocateTyped<BuildFileContext>(AllocationType_BuildFileContext, dir_entry->name, strlen(dir_entry->name), dir_entry->file_size, 0, DataSourceType::LooseSdFile)));
                            }
                        }
                    }

                    AMS_ABORT_UNLESS(num_child_dirs == cur_child_dir_ind);
                    for (s64 i = 0; i < num_child_dirs; i++) {
                        VisitDirectory(tree, program_id, dir_entry, fs, child_dirs[i]);
                    }
                }

            }

            void VisitDirectory(MergedTree *tree, DataSourceType source_type, BuildDirectoryContext *parent, u32 parent_offset, const Header &header, DirectoryTableReader &dir_table, FileTableReader &file_table) {
                const DirectoryEntry *parent_entry = dir_table.GetEntry(parent_offset);
                if (AMS_UNLIKELY(parent_entry == nullptr)) {
                    return;
                }

                u32 cur_file_offset = parent_entry->file;
                while (cur_file_offset != EmptyEntry) {
                    const FileEntry *cur_file = file_table.GetEntry(cur_file_offset);
                    if (AMS_UNLIKELY(cur_file == nullptr)) {
                        return;
                    }

                    tree->AddFile(parent, std::unique_ptr<BuildFileContext>(AllocateTyped<BuildFileContext>(AllocationType_BuildFileContext, cur_file->name, cur_file->name_size, cur_file->size, GetFileDataOffset(header, *cur_file), source_type)));

                    cur_file_offset = cur_file->sibling;
                }

                u32 cur_child_offset = parent_entry->child;
                while (cur_child_offset != EmptyEntry) {
                    BuildDirectoryContext *real_child = nullptr;
                    u32 next_child_offset = 0;
                    {
                        const DirectoryEntry *cur_child = dir_table.GetEntry(cur_child_offset);
                        if (AMS_UNLIKELY(cur_child == nullptr)) {
                            return;
                        }

                        tree->AddDirectory(std::addressof(real_child), parent, std::unique_ptr<BuildDirectoryContext>(AllocateTyped<BuildDirectoryContext>(AllocationType_BuildDirContext, cur_child->name, cur_child->name_size)));
                        AMS_ABORT_UNLESS(real_child != nullptr);

                        next_child_offset = cur_child->sibling;
                        __asm__ __volatile__("" ::: "memory");
                    }

                    VisitDirectory(tree, source_type, real_child, cur_child_offset, header, dir_table, file_table);

                    cur_child_offset = next_child_offset;
                }
            }

        }

        void AddSdFiles(MergedTree *tree, ncm::ProgramId program_id) {
            /* Open Sd Card filesystem. */
            FsFileSystem sd_filesystem;
            R_ABORT_UNLESS(fsOpenSdCardFileSystem(std::addressof(sd_filesystem)));
            ON_SCOPE_EXIT { fsFsClose(std::addressof(sd_filesystem)); };

            /* If there is no romfs folder on the SD, don't bother continuing. */
            {
                FsDir dir;
                if (R_FAILED(mitm::fs::OpenAtmosphereRomfsDirectory(std::addressof(dir), program_id, tree->GetRoot()->path, OpenDirectoryMode_Directory, std::addressof(sd_filesystem)))) {
                    return;
                }
                fsDirClose(std::addressof(dir));
            }

            ::FsDirectoryEntry dir_entry;
            VisitDirectory(tree, program_id, std::addressof(dir_entry), std::addressof(sd_filesystem), tree->GetRoot());
        }

        void AddStorageFiles(MergedTree *tree, ams::fs::IStorage *storage, DataSourceType source_type) {
            Header header;
            R_ABORT_UNLESS(storage->Read(0, std::addressof(header), sizeof(Header)));

            s64 size = 0;
            R_ABORT_UNLESS(storage->GetSize(std::addressof(size)));
            AMS_ABORT_UNLESS(IsValidHeader(header, size));

            /* Read tables. */
            DirectoryTableReader dir_table(storage, header.directory_entry_offset, header.directory_entry_size);
            FileTableReader file_table(storage, header.file_entry_offset, header.file_entry_size);

            VisitDirectory(tree, source_type, tree->GetRoot(), 0x0, header, dir_table, file_table);
        }

        namespace {

            constexpr inline size_t ChooseBucketCount(size_t num_entries) {
                if (num_entries < 3) {
                    return 3;
                } else if (num_entries < 19) {
                    return num_entries | 1;
                } else {
                    size_t count = num_entries;
                    while ((count %  2 == 0) ||
                           (count %  3 == 0) ||
                           (count %  5 == 0) ||
                           (count %  7 == 0) ||
                           (count % 11 == 0) ||
                           (count % 13 == 0) ||
                           (count % 17 == 0))
                    {
                        count++;
                    }
                    return count;
                }
            }

            class Builder {
                NON_COPYABLE(Builder);
                NON_MOVEABLE(Builder);
                private:
                    MergedTree *m_tree;
                    MetadataLayout m_layout;
                    size_t m_file_partition_size;
                public:
                    explicit Builder(MergedTree *tree);

                    void Build(SourceInfoVector *out_infos, Header *header, MetadataFile *metadata);
            };

        }

        Builder::Builder(MergedTree *tree) : m_tree(tree), m_layout{}, m_file_partition_size(0) { /* ... */ }

        void Builder::Build(SourceInfoVector *out_infos, Header *header, MetadataFile *metadata) {
            /* Clear output. */
            out_infos->clear();

            /* Open an SD card filesystem. */
            FsFileSystem sd_filesystem;
            R_ABORT_UNLESS(fsOpenSdCardFileSystem(std::addressof(sd_filesystem)));
            ON_SCOPE_EXIT { fsFsClose(std::addressof(sd_filesystem)); };

            /* Calculate hash table sizes. */
            const size_t num_dir_hash_table_entries  = ChooseBucketCount(m_tree->GetDirectoryCount());
            const size_t num_file_hash_table_entries = ChooseBucketCount(m_tree->GetFileCount());
            m_layout.dir_bucket_size  = GetBucketTableSize(num_dir_hash_table_entries);
            m_layout.dir_entry_size   = m_tree->GetDirectoryTableSize();
            m_layout.file_bucket_size = GetBucketTableSize(num_file_hash_table_entries);
            m_layout.file_entry_size  = m_tree->GetFileTableSize();

            std::memset(header, 0x00, sizeof(*header));

            /* Open metadata file. */
            MetadataWriter writer(metadata);
            writer.Create(m_layout);

            /* Ensure later hash tables will have correct defaults. */
            static_assert(EmptyEntry == 0xFFFFFFFF);

            /* Emplace metadata source info. */
            out_infos->emplace_back(0, sizeof(*header), DataSourceType::Memory, header);

            /* Process Files. */
            {
                u32 entry_offset = 0;
                BuildFileContext *prev_file = nullptr;
                for (BuildFileContext *cur_file : m_tree->GetFiles()) {
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
                for (BuildFileContext *cur_file : m_tree->GetFiles() | std::views::reverse) {
                    cur_file->sibling = cur_file->parent->file;
                    cur_file->parent->file = cur_file;
                }
            }

            /* Process Directories. */
            {
                u32 entry_offset = 0;
                for (BuildDirectoryContext *cur_dir : m_tree->GetDirectories()) {
                    cur_dir->entry_offset = entry_offset;
                    entry_offset += EntrySize<DirectoryEntry>(cur_dir->path_len);
                }
                /* Assign deferred parent/sibling ownership. */
                for (BuildDirectoryContext *cur_dir : m_tree->GetDirectories() | std::views::reverse) {
                    if (cur_dir == m_tree->GetRoot()) {
                        continue;
                    }
                    cur_dir->sibling = cur_dir->parent->child;
                    cur_dir->parent->child = cur_dir;
                }
            }

            /* Set all files' hash value = hash index. */
            for (BuildFileContext *cur_file : m_tree->GetFiles()) {
                cur_file->hash_value = GetBucketIndex(cur_file->parent->entry_offset, cur_file->path, cur_file->path_len, num_file_hash_table_entries);
            }

            /* Set all directories' hash value = hash index. */
            for (BuildDirectoryContext *cur_dir : m_tree->GetDirectories()) {
                cur_dir->hash_value = GetBucketIndex(cur_dir == m_tree->GetRoot() ? RootEntryOffset : cur_dir->parent->entry_offset, cur_dir->path, cur_dir->path_len, num_dir_hash_table_entries);
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

                    for (BuildFileContext *cur_file : m_tree->GetFiles()) {
                        if (cur_file->HasHashMark()) {
                            continue;
                        }

                        if (const auto hash_ind = cur_file->hash_value; ofs_ind <= hash_ind && hash_ind < end_ind) {
                            cur_file->hash_value = hash_table[hash_ind - ofs_ind];
                            hash_table[hash_ind - ofs_ind] = cur_file->entry_offset;

                            cur_file->SetHashMark();
                        }
                    }

                    writer.AppendFileBuckets(hash_table, std::min(m_layout.file_bucket_size - ofs, hash_table_size));
                }

                /* Write the directory hash table. */
                for (size_t ofs = 0; ofs < m_layout.dir_bucket_size; ofs += hash_table_size) {
                    std::memset(hash_table, 0xFF, hash_table_size);

                    const u32 ofs_ind = ofs / sizeof(u32);
                    const u32 end_ind = (ofs + hash_table_size) / sizeof(u32);

                    for (BuildDirectoryContext *cur_dir : m_tree->GetDirectories()) {
                        if (cur_dir->HasHashMark()) {
                            continue;
                        }

                        if (const auto hash_ind = cur_dir->hash_value; ofs_ind <= hash_ind && hash_ind < end_ind) {
                            cur_dir->hash_value = hash_table[hash_ind - ofs_ind];
                            hash_table[hash_ind - ofs_ind] = cur_dir->entry_offset;

                            cur_dir->SetHashMark();
                        }
                    }

                    writer.AppendDirectoryBuckets(hash_table, std::min(m_layout.dir_bucket_size - ofs, hash_table_size));
                }
            }

            /* Replace sibling pointers with sibling entry_offsets, so that we can de-allocate as we go. */
            {
                /* Set all directories sibling and file pointers. */
                for (BuildDirectoryContext *cur_dir : m_tree->GetDirectories()) {
                    cur_dir->ClearHashMark();

                    cur_dir->sibling_offset = (cur_dir->sibling == nullptr) ? EmptyEntry : cur_dir->sibling->entry_offset;
                    cur_dir->child_offset   = (cur_dir->child   == nullptr) ? EmptyEntry : cur_dir->child->entry_offset;
                    cur_dir->file_offset    = (cur_dir->file    == nullptr) ? EmptyEntry : cur_dir->file->entry_offset;

                    cur_dir->parent_offset  = cur_dir == m_tree->GetRoot() ? RootEntryOffset : cur_dir->parent->entry_offset;
                }

                /* Replace all files' sibling pointers. */
                for (BuildFileContext *cur_file : m_tree->GetFiles()) {
                    cur_file->ClearHashMark();

                    cur_file->sibling_offset = (cur_file->sibling == nullptr) ? EmptyEntry : cur_file->sibling->entry_offset;
                }
            }

            /* Write the file table. */
            {
                while (BuildFileContext *cur_file = m_tree->GetFrontFile()) {
                    FileEntry *cur_entry = writer.AppendFileEntry(cur_file->path_len);

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
                                    out_infos->emplace_back(cur_file->offset + FilePartitionOffset, cur_file->size, cur_file->source_type, cur_file->orig_offset);
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

                    m_tree->RemoveFrontFile();
                }

                writer.FinishFileTable();
            }

            /* Write the directory table. */
            {
                while (BuildDirectoryContext *cur_dir = m_tree->GetFrontDirectory()) {
                    DirectoryEntry *cur_entry = writer.AppendDirectoryEntry(cur_dir->path_len);

                    /* Set entry fields. */
                    cur_entry->parent  = cur_dir->parent_offset;
                    cur_entry->sibling = cur_dir->sibling_offset;
                    cur_entry->child   = cur_dir->child_offset;
                    cur_entry->file    = cur_dir->file_offset;
                    cur_entry->hash    = cur_dir->hash_value;

                    /* Set name. */
                    SetEntryName(cur_entry, cur_dir->path, cur_dir->path_len);

                    m_tree->RemoveFrontDirectory();
                }

                writer.FinishDirectoryTable();
            }

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
                writer.Finish();
                out_infos->emplace_back(header->directory_bucket_offset, m_layout.TotalSize(), DataSourceType::Metadata, metadata);
            }
        }

        void Build(MergedTree *tree, SourceInfoVector *out_infos, Header *header, MetadataFile *metadata) {
            Builder builder(tree);
            builder.Build(out_infos, header, metadata);
        }

    }

}
