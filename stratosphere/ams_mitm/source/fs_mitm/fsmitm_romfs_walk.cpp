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
#include "fsmitm_romfs_walk.hpp"
#include "fsmitm_romfs_format.hpp"
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

            void VisitDirectory(LayeredTree *tree, ncm::ProgramId program_id, ::FsDirectoryEntry *dir_entry, FsFileSystem *fs, BuildDirectoryContext *parent) {
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
                    BuildDirectoryContext ** const child_dirs = num_child_dirs != 0 ? AllocateTracked<BuildDirectoryContext *>(AllocationType_DirPointerArray, sizeof(BuildDirectoryContext *) * num_child_dirs) : nullptr;
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

                                BuildDirectoryContext * const real_child = tree->AddDirectory(parent, dir_entry->name, strlen(dir_entry->name));
                                child_dirs[cur_child_dir_ind++] = real_child;
                                AMS_ABORT_UNLESS(cur_child_dir_ind <= num_child_dirs);
                            } else /* if (dir_entry->type == FsDirEntryType_File) */ {
                                tree->AddFile(parent, dir_entry->name, strlen(dir_entry->name), dir_entry->file_size, 0, DataSourceType::LooseSdFile);
                            }
                        }
                    }

                    AMS_ABORT_UNLESS(num_child_dirs == cur_child_dir_ind);
                    for (s64 i = 0; i < num_child_dirs; i++) {
                        VisitDirectory(tree, program_id, dir_entry, fs, child_dirs[i]);
                    }
                }

            }

            void VisitDirectory(LayeredTree *tree, DataSourceType source_type, BuildDirectoryContext *parent, u32 parent_offset, const Header &header, DirectoryTableReader &dir_table, FileTableReader &file_table) {
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

                    tree->AddFile(parent, cur_file->name, cur_file->name_size, cur_file->size, GetFileDataOffset(header, *cur_file), source_type);

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

                        real_child = tree->AddDirectory(parent, cur_child->name, cur_child->name_size);

                        next_child_offset = cur_child->sibling;
                        __asm__ __volatile__("" ::: "memory");
                    }

                    VisitDirectory(tree, source_type, real_child, cur_child_offset, header, dir_table, file_table);

                    cur_child_offset = next_child_offset;
                }
            }

        }

        void AddSdFiles(LayeredTree *tree, ncm::ProgramId program_id) {
            /* Open Sd Card filesystem. */
            FsFileSystem sd_filesystem;
            R_ABORT_UNLESS(fsOpenSdCardFileSystem(std::addressof(sd_filesystem)));
            ON_SCOPE_EXIT { fsFsClose(std::addressof(sd_filesystem)); };

            /* If there is no romfs folder on the SD, don't bother continuing. */
            {
                FsDir dir;
                if (R_FAILED(mitm::fs::OpenAtmosphereRomfsDirectory(std::addressof(dir), program_id, tree->GetRoot()->name, OpenDirectoryMode_Directory, std::addressof(sd_filesystem)))) {
                    return;
                }
                fsDirClose(std::addressof(dir));
            }

            ::FsDirectoryEntry dir_entry;
            VisitDirectory(tree, program_id, std::addressof(dir_entry), std::addressof(sd_filesystem), tree->GetRoot());
        }

        void AddStorageFiles(LayeredTree *tree, ams::fs::IStorage *storage, DataSourceType source_type) {
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

    }

}
