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
#include "fsmitm_romfs_layered_files.hpp"
#include "fsmitm_romfs_format.hpp"
#include "fsmitm_romfs_metadata.hpp"
#include "fsmitm_romfs_tables.hpp"
#include "fsmitm_romfs_tree.hpp"

namespace ams::mitm::fs {

    using namespace ams::fs;

    namespace romfs {

        namespace {

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
                        file->starts_source = true;
                        if (m_prev_type == file->type && is_storage_or_file) {
                            const s64 expected = m_size - m_prev_offset + m_prev_orig_offset;
                            AMS_ABORT_UNLESS(expected <= file->orig_offset);
                            m_size += file->orig_offset - expected;
                            file->starts_source = false;
                        }

                        file->offset = m_size;
                        m_size += file->size;

                        m_prev_type        = file->type;
                        m_prev_offset      = file->offset;
                        m_prev_orig_offset = file->orig_offset;
                    }
            };

            size_t PlaceFiles(LayeredTree *tree, MetadataLayout *layout, s64 *out_partition_size) {
                /* Process Files. */
                size_t num_file_sources = 0;
                size_t num_files  = 0;
                size_t table_size = 0;
                PartitionLayout partition;
                for (BuildFileContext * const cur_file : tree->GetFiles()) {
                    PlacedFile placed = { .type = cur_file->source_type, .size = cur_file->size, .orig_offset = cur_file->orig_offset };
                    partition.Place(std::addressof(placed));
                    num_file_sources += placed.starts_source;

                    /* Calculate offsets. */
                    cur_file->offset = placed.offset;
                    cur_file->entry_offset = static_cast<u32>(table_size);
                    table_size += EntrySize<FileEntry>(cur_file->name_len);
                    ++num_files;
                }
                *out_partition_size = partition.GetSize();
                /* Assign deferred parent/sibling ownership. */
                for (BuildFileContext * const cur_file : tree->GetFiles() | std::views::reverse) {
                    cur_file->sibling = cur_file->parent->file;
                    cur_file->parent->file = cur_file;
                }

                AMS_ABORT_UNLESS(table_size <= MaxTableSize);
                layout->file_entry_size  = table_size;
                layout->file_bucket_size = GetBucketTableSize(ChooseBucketCount(num_files));
                return num_file_sources;
            }

            void DecoupleContexts(LayeredTree *tree) {
                /* Replace sibling pointers with sibling entry_offsets, so that we can de-allocate as we go. */
                /* Set all directories file offsets. */
                for (BuildDirectoryContext * const cur_dir : tree->GetDirectories()) {
                    cur_dir->file_offset    = (cur_dir->file    == nullptr) ? EmptyEntry : cur_dir->file->entry_offset;
                }

                /* Replace all files' sibling pointers. */
                for (BuildFileContext * const cur_file : tree->GetFiles()) {
                    cur_file->ClearHashMark();

                    cur_file->sibling_offset = (cur_file->sibling == nullptr) ? EmptyEntry : cur_file->sibling->entry_offset;
                }
            }

            void WriteFileTable(LayeredTree *tree, SourceInfoChunks *out_infos, MetadataWriter *writer) {
                const BuildDirectoryContext *loose_parent = nullptr;
                const LooseDirectory *loose_dir = nullptr;
                while (BuildFileContext * const cur_file = tree->GetFrontFile()) {
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

                                    loose_dir    = out_infos->AddLooseDirectory(dir_path, dir_len);
                                    loose_parent = cur_file->parent;
                                }
                                AMS_ABORT_UNLESS(GetChildPathLength(loose_dir->path_len, cur_file->name_len) <= fs::EntryNameLengthMax);

                                const LoosePath * const path = out_infos->AddLooseFile(loose_dir, cur_file->name, cur_file->name_len);
                                out_infos->EmplaceBack(cur_file->offset + FilePartitionOffset, cur_file->size, cur_file->source_type, path);
                            }
                            break;
                        AMS_UNREACHABLE_DEFAULT_CASE();
                    }

                    tree->RemoveFrontFile();
                }

                writer->FinishFileTable();
            }

        }

        MeasuredFiles SortAndMeasureFiles(LayeredTree *tree, MetadataLayout *layout) {
            s64 partition_size = 0;
            const size_t sources = PlaceFiles(tree, layout, std::addressof(partition_size));
            return { sources, partition_size };
        }

        void WriteFileTableAndSources(LayeredTree *tree, const MeasuredFiles &measured, MetadataWriter *writer, const MetadataLayout &layout, SourceInfoChunks *out_infos) {
            const size_t sources_before = out_infos->GetCount();

            WriteBuckets(writer, &MetadataWriter::AppendFileBuckets, tree->GetFiles(), layout.file_bucket_size);

            DecoupleContexts(tree);
            WriteFileTable(tree, out_infos, writer);

            AMS_ABORT_UNLESS(out_infos->GetCount() - sources_before == measured.sources);
        }

    }

}
