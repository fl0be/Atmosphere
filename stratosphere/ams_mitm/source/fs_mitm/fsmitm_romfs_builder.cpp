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
#include "fsmitm_romfs_layered_files.hpp"
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

            void PlaceDirectories(LayeredTree *tree, MetadataLayout *layout) {
                size_t num_dirs   = 0;
                size_t table_size = 0;
                for (BuildDirectoryContext * const cur_dir : tree->GetDirectories()) {
                    cur_dir->entry_offset = static_cast<u32>(table_size);
                    table_size += EntrySize<DirectoryEntry>(cur_dir->name_len);
                    ++num_dirs;

                    if (cur_dir->parent != nullptr) {
                        BuildDirectoryContext * const parent = cur_dir->parent;
                        if (parent->child != nullptr) {
                            parent->child->sibling_entry_offset = cur_dir->entry_offset;
                        } else {
                            parent->child_entry_offset = cur_dir->entry_offset;
                        }
                        parent->child = cur_dir;
                    }
                }

                AMS_ABORT_UNLESS(table_size <= MaxTableSize);
                layout->dir_entry_size  = table_size;
                layout->dir_bucket_size = GetBucketTableSize(ChooseBucketCount(num_dirs));
            }

            void WriteDirectoryTable(const LayeredTree &tree, MetadataWriter *writer, const MetadataLayout &layout) {
                BucketTable buckets(AllocationType_TableCache, GetBucketCount(layout.dir_bucket_size));
                for (const BuildDirectoryContext * const cur_dir : tree.GetDirectories()) {
                    DirectoryEntry * const cur_entry = writer->AppendDirectoryEntry(cur_dir->name_len);

                    const u32 parent_offset = (cur_dir->parent != nullptr) ? cur_dir->parent->entry_offset : RootEntryOffset;

                    /* Set entry fields. */
                    cur_entry->parent  = parent_offset;
                    cur_entry->sibling = cur_dir->sibling_entry_offset;
                    cur_entry->child   = cur_dir->child_entry_offset;
                    cur_entry->file    = cur_dir->file_offset;
                    buckets.ChainEntry(cur_entry, cur_dir->entry_offset, parent_offset, cur_dir->name, cur_dir->name_len);

                    /* Set name. */
                    SetEntryName(cur_entry, cur_dir->name, cur_dir->name_len);
                }

                writer->WriteDirectoryBuckets(buckets);
            }

            void FillHeader(Header *header, const MetadataLayout &layout, s64 file_partition_size) {
                header->size                    = sizeof(*header);
                header->file_bucket_size        = layout.file_bucket_size;
                header->file_entry_size         = layout.file_entry_size;
                header->directory_bucket_size   = layout.dir_bucket_size;
                header->directory_entry_size    = layout.dir_entry_size;
                header->body_offset             = FilePartitionOffset;

                const s64 metadata_begin        = AlignMetadataOffset(FilePartitionOffset + file_partition_size);
                header->directory_bucket_offset = metadata_begin + layout.DirBucketOffset();
                header->directory_entry_offset  = metadata_begin + layout.DirEntryOffset();
                header->file_bucket_offset      = metadata_begin + layout.FileBucketOffset();
                header->file_entry_offset       = metadata_begin + layout.FileEntryOffset();
            }

        }

        void Build(LayeredTree *tree, SourceInfoChunks *out_infos, Header *header, MetadataFile *metadata) {
            MetadataLayout layout = {};
            PlaceDirectories(tree, std::addressof(layout));
            const MeasuredFiles files = SortAndMeasureFiles(tree, std::addressof(layout));

            /* Open metadata file. */
            MetadataWriter writer(metadata);
            writer.Create(layout);

            out_infos->Initialize(HeaderSources + files.sources + MetadataSources);

            /* Emplace header source info. */
            out_infos->EmplaceBack(0, sizeof(*header), DataSourceType::Memory, header);

            WriteFileTableAndSources(tree, files, std::addressof(writer), layout, out_infos);
            WriteDirectoryTable(*tree, std::addressof(writer), layout);

            FillHeader(header, layout, files.partition_size);

            /* Save metadata to the SD card, to save on memory space. */
            {
                writer.Finish();
                out_infos->EmplaceBack(header->directory_bucket_offset, layout.TotalSize(), DataSourceType::Metadata, metadata);
            }
        }

    }

}
