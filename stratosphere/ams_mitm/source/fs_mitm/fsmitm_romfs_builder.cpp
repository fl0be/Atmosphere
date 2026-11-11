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
#include "fsmitm_romfs_metadata.hpp"
#include "fsmitm_romfs_tables.hpp"
#include "fsmitm_romfs_tree.hpp"

namespace ams::mitm::fs {

    using namespace ams::fs;

    namespace romfs {

        namespace {

            constexpr inline size_t HeaderSources   = 1;
            constexpr inline size_t MetadataSources = 1;

            template<typename Contexts>
            void WriteBuckets(MetadataWriter *writer, void (MetadataWriter::*append)(const void *, size_t), Contexts contexts, size_t bucket_size) {
                const size_t num_buckets = GetBucketCount(bucket_size);
                for (auto * const cur_ctx : contexts) {
                    cur_ctx->hash_value = GetBucketIndex(cur_ctx->parent == nullptr ? RootEntryOffset : cur_ctx->parent->entry_offset, cur_ctx->name, cur_ctx->name_len, num_buckets);
                }

                HashTableStorage hash_table_storage(bucket_size);

                u32 *hash_table = hash_table_storage.GetBuffer();
                size_t hash_table_size = hash_table_storage.GetBufferSize();

                for (size_t ofs = 0; ofs < bucket_size; ofs += hash_table_size) {
                    std::memset(hash_table, 0xFF, hash_table_size);

                    const u32 ofs_ind = ofs / sizeof(u32);
                    const u32 end_ind = (ofs + hash_table_size) / sizeof(u32);

                    for (auto * const cur_ctx : contexts) {
                        if (cur_ctx->HasHashMark()) {
                            continue;
                        }

                        if (const auto hash_ind = cur_ctx->hash_value; ofs_ind <= hash_ind && hash_ind < end_ind) {
                            cur_ctx->hash_value = hash_table[hash_ind - ofs_ind];
                            hash_table[hash_ind - ofs_ind] = cur_ctx->entry_offset;

                            cur_ctx->SetHashMark();
                        }
                    }

                    (writer->*append)(hash_table, std::min(bucket_size - ofs, hash_table_size));
                }
            }

            struct PlacedFile {
                DataSourceType type;
                s64 size;
                s64 orig_offset;
                s64 offset;
                bool starts_source;
            };

            class PartitionLayout {
                private:
                    s64 m_size;
                    util::optional<DataSourceType> m_prev_type;
                    s64 m_prev_offset;
                    s64 m_prev_orig_offset;
                public:
                    PartitionLayout() : m_size(0), m_prev_type(), m_prev_offset(0), m_prev_orig_offset(0) { /* ... */ }

                    s64 GetSize() const { return m_size; }

                    void Place(PlacedFile *file) {
                        m_size = AlignFileContent(m_size);

                        /* Check if extra padding is present in original source, preserve it to make our life easier. */
                        const bool is_storage_or_file = file->type == DataSourceType::Storage || file->type == DataSourceType::File;
                        file->starts_source = !(m_prev_type == file->type && is_storage_or_file);
                        if (!file->starts_source) {
                            const s64 expected = m_size - m_prev_offset + m_prev_orig_offset;
                            if (expected != file->orig_offset) {
                                AMS_ABORT_UNLESS(expected <= file->orig_offset);
                                m_size += file->orig_offset - expected;
                            }
                        }

                        file->offset = m_size;
                        m_size += file->size;

                        m_prev_type        = file->type;
                        m_prev_offset      = file->offset;
                        m_prev_orig_offset = file->orig_offset;
                    }
            };

            class Builder {
                NON_COPYABLE(Builder);
                NON_MOVEABLE(Builder);
                private:
                    LayeredTree * const m_tree;
                    MetadataLayout m_layout;
                    size_t m_file_partition_size;
                public:
                    explicit Builder(LayeredTree *tree);

                    void Build(SourceInfoChunks *out_infos, Header *header, MetadataFile *metadata);
                private:
                    size_t PlaceFiles();
                    void PlaceDirectories();
                    void DecoupleContexts();
                    void WriteFileTable(SourceInfoChunks *out_infos, MetadataWriter *writer);
                    void WriteDirectoryTable(MetadataWriter *writer);
                    void FillHeader(Header *header) const;
            };

        }

        Builder::Builder(LayeredTree *tree) : m_tree(tree), m_layout{}, m_file_partition_size(0) { /* ... */ }

        void Builder::Build(SourceInfoChunks *out_infos, Header *header, MetadataFile *metadata) {
            /* Open an SD card filesystem. */
            FsFileSystem sd_filesystem;
            R_ABORT_UNLESS(fsOpenSdCardFileSystem(std::addressof(sd_filesystem)));
            ON_SCOPE_EXIT { fsFsClose(std::addressof(sd_filesystem)); };

            std::memset(header, 0x00, sizeof(*header));

            const size_t num_file_sources = this->PlaceFiles();
            this->PlaceDirectories();

            /* Open metadata file. */
            MetadataWriter writer(metadata);
            writer.Create(m_layout);

            out_infos->Initialize(HeaderSources + num_file_sources + MetadataSources);

            /* Emplace metadata source info. */
            out_infos->EmplaceBack(0, sizeof(*header), DataSourceType::Memory, header);

            WriteBuckets(std::addressof(writer), &MetadataWriter::AppendFileBuckets,      m_tree->GetFiles(),       m_layout.file_bucket_size);
            WriteBuckets(std::addressof(writer), &MetadataWriter::AppendDirectoryBuckets, m_tree->GetDirectories(), m_layout.dir_bucket_size);

            this->DecoupleContexts();
            this->WriteFileTable(out_infos, std::addressof(writer));
            this->WriteDirectoryTable(std::addressof(writer));

            this->FillHeader(header);

            /* Save metadata to the SD card, to save on memory space. */
            {
                writer.Finish();
                out_infos->EmplaceBack(header->directory_bucket_offset, m_layout.TotalSize(), DataSourceType::Metadata, metadata);
            }
        }

        size_t Builder::PlaceFiles() {
            /* Process Files. */
            size_t num_file_sources = 0;
            size_t num_files  = 0;
            size_t table_size = 0;
            PartitionLayout partition;
            for (BuildFileContext * const cur_file : m_tree->GetFiles()) {
                PlacedFile placed = { .type = cur_file->source_type, .size = cur_file->size, .orig_offset = cur_file->orig_offset };
                partition.Place(std::addressof(placed));
                num_file_sources += placed.starts_source;

                /* Calculate offsets. */
                cur_file->offset = placed.offset;
                cur_file->entry_offset = static_cast<u32>(table_size);
                table_size += EntrySize<FileEntry>(cur_file->name_len);
                ++num_files;
            }
            m_file_partition_size = partition.GetSize();
            /* Assign deferred parent/sibling ownership. */
            for (BuildFileContext * const cur_file : m_tree->GetFiles() | std::views::reverse) {
                cur_file->sibling = cur_file->parent->file;
                cur_file->parent->file = cur_file;
            }

            AMS_ABORT_UNLESS(table_size <= MaxTableSize);
            m_layout.file_entry_size  = table_size;
            m_layout.file_bucket_size = GetBucketTableSize(ChooseBucketCount(num_files));
            return num_file_sources;
        }

        void Builder::PlaceDirectories() {
            size_t num_dirs   = 0;
            size_t table_size = 0;
            for (BuildDirectoryContext * const cur_dir : m_tree->GetDirectories()) {
                cur_dir->entry_offset = static_cast<u32>(table_size);
                table_size += EntrySize<DirectoryEntry>(cur_dir->name_len);
                ++num_dirs;
            }
            /* Assign deferred parent/sibling ownership. */
            for (BuildDirectoryContext * const cur_dir : m_tree->GetDirectories() | std::views::reverse) {
                if (cur_dir == m_tree->GetRoot()) {
                    continue;
                }
                cur_dir->sibling = cur_dir->parent->child;
                cur_dir->parent->child = cur_dir;
            }

            AMS_ABORT_UNLESS(table_size <= MaxTableSize);
            m_layout.dir_entry_size  = table_size;
            m_layout.dir_bucket_size = GetBucketTableSize(ChooseBucketCount(num_dirs));
        }

        void Builder::DecoupleContexts() {
            /* Replace sibling pointers with sibling entry_offsets, so that we can de-allocate as we go. */
            /* Set all directories sibling and file pointers. */
            for (BuildDirectoryContext * const cur_dir : m_tree->GetDirectories()) {
                cur_dir->ClearHashMark();

                cur_dir->sibling_offset = (cur_dir->sibling == nullptr) ? EmptyEntry : cur_dir->sibling->entry_offset;
                cur_dir->child_offset   = (cur_dir->child   == nullptr) ? EmptyEntry : cur_dir->child->entry_offset;
                cur_dir->file_offset    = (cur_dir->file    == nullptr) ? EmptyEntry : cur_dir->file->entry_offset;

                cur_dir->parent_offset  = cur_dir == m_tree->GetRoot() ? RootEntryOffset : cur_dir->parent->entry_offset;
            }

            /* Replace all files' sibling pointers. */
            for (BuildFileContext * const cur_file : m_tree->GetFiles()) {
                cur_file->ClearHashMark();

                cur_file->sibling_offset = (cur_file->sibling == nullptr) ? EmptyEntry : cur_file->sibling->entry_offset;
            }
        }

        void Builder::WriteFileTable(SourceInfoChunks *out_infos, MetadataWriter *writer) {
            const BuildDirectoryContext *loose_parent = nullptr;
            const LooseDirectory *loose_dir = nullptr;
            while (BuildFileContext * const cur_file = m_tree->GetFrontFile()) {
                FileEntry * const cur_entry = writer->AppendFileEntry(cur_file->name_len);

                /* Set entry fields. */
                cur_entry->parent  = cur_file->parent->entry_offset;
                cur_entry->sibling = cur_file->sibling_offset;
                cur_entry->offset  = cur_file->offset;
                cur_entry->size    = cur_file->size;
                cur_entry->hash    = cur_file->hash_value;

                /* Set name. */
                SetEntryName(cur_entry, cur_file->name, cur_file->name_len);

                /* Emplace a source. */
                switch (cur_file->source_type) {
                    case DataSourceType::Storage:
                    case DataSourceType::File:
                        {
                            /* Try to compact if possible. */
                            if (out_infos->Back().source_type == cur_file->source_type) {
                                out_infos->ExtendBackTo(cur_file->offset + FilePartitionOffset + cur_file->size);
                            } else {
                                out_infos->EmplaceBack(cur_file->offset + FilePartitionOffset, cur_file->size, cur_file->source_type, cur_file->orig_offset);
                            }
                        }
                        break;
                    case DataSourceType::LooseSdFile:
                        {
                            if (cur_file->parent != loose_parent) {
                                char dir_path[fs::EntryNameLengthMax + 1];
                                AMS_ABORT_UNLESS(cur_file->parent->GetPathLength() <= fs::EntryNameLengthMax);
                                const size_t dir_len = cur_file->parent->GetPath(dir_path);

                                loose_dir    = out_infos->GetLoosePathBlocks().AddDirectory(dir_path, dir_len);
                                loose_parent = cur_file->parent;
                            }
                            AMS_ABORT_UNLESS(GetChildPathLength(loose_dir->path_len, cur_file->name_len) <= fs::EntryNameLengthMax);

                            LoosePath * const path = out_infos->GetLoosePathBlocks().AddFile(loose_dir, cur_file->name, cur_file->name_len);
                            out_infos->EmplaceBack(cur_file->offset + FilePartitionOffset, cur_file->size, cur_file->source_type, path);
                        }
                        break;
                    AMS_UNREACHABLE_DEFAULT_CASE();
                }

                m_tree->RemoveFrontFile();
            }

            writer->FinishFileTable();
        }

        void Builder::WriteDirectoryTable(MetadataWriter *writer) {
            while (BuildDirectoryContext * const cur_dir = m_tree->GetFrontDirectory()) {
                DirectoryEntry * const cur_entry = writer->AppendDirectoryEntry(cur_dir->name_len);

                /* Set entry fields. */
                cur_entry->parent  = cur_dir->parent_offset;
                cur_entry->sibling = cur_dir->sibling_offset;
                cur_entry->child   = cur_dir->child_offset;
                cur_entry->file    = cur_dir->file_offset;
                cur_entry->hash    = cur_dir->hash_value;

                /* Set name. */
                SetEntryName(cur_entry, cur_dir->name, cur_dir->name_len);

                m_tree->RemoveFrontDirectory();
            }

            writer->FinishDirectoryTable();
        }

        void Builder::FillHeader(Header *header) const {
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
        }

        void Build(LayeredTree *tree, SourceInfoChunks *out_infos, Header *header, MetadataFile *metadata) {
            Builder builder(tree);
            builder.Build(out_infos, header, metadata);
        }

    }

}
