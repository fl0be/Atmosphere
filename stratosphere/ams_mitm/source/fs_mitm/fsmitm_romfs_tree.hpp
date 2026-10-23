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
#include "fsmitm_romfs_sources.hpp"
#include "fsmitm_romfs_format.hpp"

namespace ams::mitm::fs::romfs {

    struct BuildFileContext;

    struct BuildDirectoryContext {
        NON_COPYABLE(BuildDirectoryContext);
        NON_MOVEABLE(BuildDirectoryContext);

        char *path;
        union {
            BuildDirectoryContext *parent;
        };
        union {
            BuildDirectoryContext *child;
            struct {
                u32 parent_offset;
                u32 child_offset;
            };
        };
        union {
            BuildDirectoryContext *sibling;
            u32 sibling_offset;
        };
        union {
            BuildFileContext *file;
            u32 file_offset;
        };
        u32 path_len;
        u32 entry_offset;
        u32 hash_value;

        struct RootTag{};

        BuildDirectoryContext(RootTag) : parent(nullptr), child(nullptr), sibling(nullptr), file(nullptr), path_len(0), entry_offset(0), hash_value(EmptyEntry) {
            this->path = AllocateTracked<char>(AllocationType_DirName, 1);
            this->path[0] = '\x00';
        }

        BuildDirectoryContext(const char *entry_name, size_t entry_name_len) : parent(nullptr), child(nullptr), sibling(nullptr), file(nullptr), entry_offset(0) {
            this->path_len = entry_name_len;
            this->path = AllocateTracked<char>(AllocationType_DirName, this->path_len + 1);
            std::memcpy(this->path, entry_name, entry_name_len);
            this->path[this->path_len] = '\x00';
        }

        ~BuildDirectoryContext() {
            if (this->path != nullptr) {
                FreeTracked(AllocationType_DirName, this->path, this->path_len + 1);
                this->path = nullptr;
            }
        }

        void operator delete(void *p) {
            FreeTracked(AllocationType_BuildDirContext, p, sizeof(BuildDirectoryContext));
        }

        size_t GetPathLength() const {
            if (this->parent == nullptr) {
                return 0;
            }

            return GetChildPathLength(this->parent->GetPathLength(), this->path_len);
        }

        size_t GetPath(char *dst) const {
            if (this->parent == nullptr) {
                dst[0] = '\x00';
                return 0;
            }

            const size_t parent_len = this->parent->GetPath(dst);
            const size_t len = GetChildPathLength(parent_len, this->path_len);
            dst[parent_len] = '/';
            std::memcpy(dst + parent_len + 1, this->path, this->path_len);
            dst[len] = '\x00';
            return len;
        }

        bool HasHashMark() const {
            return reinterpret_cast<uintptr_t>(this->sibling) & UINT64_C(0x8000000000000000);
        }

        void SetHashMark() {
            this->sibling = reinterpret_cast<BuildDirectoryContext *>(reinterpret_cast<uintptr_t>(this->sibling) | UINT64_C(0x8000000000000000));
        }

        void ClearHashMark() {
            this->sibling = reinterpret_cast<BuildDirectoryContext *>(reinterpret_cast<uintptr_t>(this->sibling) & ~UINT64_C(0x8000000000000000));
        }
    };

    struct BuildFileContext {
        NON_COPYABLE(BuildFileContext);
        NON_MOVEABLE(BuildFileContext);

        char *path;
        BuildDirectoryContext *parent;
        union {
            BuildFileContext *sibling;
            u32 sibling_offset;
        };
        s64 offset;
        s64 size;
        s64 orig_offset;
        u32 path_len;
        u32 entry_offset;
        u32 hash_value;
        DataSourceType source_type;

        BuildFileContext(const char *entry_name, size_t entry_name_len, s64 sz, s64 o_o, DataSourceType type) : parent(nullptr), sibling(nullptr), offset(0), size(sz), orig_offset(o_o), entry_offset(0), hash_value(EmptyEntry), source_type(type) {
            this->path_len = entry_name_len;
            this->path = AllocateTracked<char>(AllocationType_FileName, this->path_len + 1);
            std::memcpy(this->path, entry_name, entry_name_len);
            this->path[this->path_len] = 0;
        }

        ~BuildFileContext() {
            if (this->path != nullptr) {
                FreeTracked(AllocationType_FileName, this->path, this->path_len + 1);
                this->path = nullptr;
            }
        }

        void operator delete(void *p) {
            FreeTracked(AllocationType_BuildFileContext, p, sizeof(BuildFileContext));
        }

        size_t GetPathLength() const {
            if (this->parent == nullptr) {
                return 0;
            }

            return GetChildPathLength(this->parent->GetPathLength(), this->path_len);
        }

        size_t GetPath(char *dst) const {
            if (this->parent == nullptr) {
                dst[0] = '\x00';
                return 0;
            }

            const size_t parent_len = this->parent->GetPath(dst);
            const size_t len = GetChildPathLength(parent_len, this->path_len);
            dst[parent_len] = '/';
            std::memcpy(dst + parent_len + 1, this->path, this->path_len);
            dst[len] = '\x00';
            return len;
        }

        bool HasHashMark() const {
            return reinterpret_cast<uintptr_t>(this->sibling) & UINT64_C(0x8000000000000000);
        }

        void SetHashMark() {
            this->sibling = reinterpret_cast<BuildFileContext *>(reinterpret_cast<uintptr_t>(this->sibling) | UINT64_C(0x8000000000000000));
        }

        void ClearHashMark() {
            this->sibling = reinterpret_cast<BuildFileContext *>(reinterpret_cast<uintptr_t>(this->sibling) & ~UINT64_C(0x8000000000000000));
        }
    };

    class DirectoryTableReader;
    class FileTableReader;

    class MergedTree {
        NON_COPYABLE(MergedTree);
        NON_MOVEABLE(MergedTree);
        friend class Builder;
        private:
            template<typename T>
            struct Comparator {
                static constexpr inline int Compare(const char *a, const char *b) {
                    unsigned char c1{}, c2{};
                    while ((c1 = *a++) == (c2 = *b++)) {
                        if (c1 == '\x00') {
                            return 0;
                        }
                    }
                    return (c1 - c2);
                }

                constexpr bool operator()(const std::unique_ptr<T> &lhs, const std::unique_ptr<T> &rhs) const {
                    char lhs_path[ams::fs::EntryNameLengthMax + 1];
                    char rhs_path[ams::fs::EntryNameLengthMax + 1];
                    lhs->GetPath(lhs_path);
                    rhs->GetPath(rhs_path);
                    return Comparator::Compare(lhs_path, rhs_path) < 0;
                }
            };

            template<AllocationType AllocType, typename T>
            using ContextSet = std::set<std::unique_ptr<T>, Comparator<T>, TrackedAllocator<AllocType, std::unique_ptr<T>>>;
        private:
            util::TypedStorage<ScopedHeapLock> m_heap_lock;
            ncm::ProgramId m_program_id;
            BuildDirectoryContext *m_root;
            ContextSet<AllocationType_DirContextSet, BuildDirectoryContext> m_directories;
            ContextSet<AllocationType_FileContextSet, BuildFileContext> m_files;
            size_t m_num_dirs;
            size_t m_num_files;
            size_t m_dir_table_size;
            size_t m_file_table_size;

            ::FsDirectoryEntry m_dir_entry;
            DataSourceType m_cur_source_type;
        private:
            void VisitDirectory(FsFileSystem *fs, BuildDirectoryContext *parent);
            void VisitDirectory(BuildDirectoryContext *parent, u32 parent_offset, DirectoryTableReader &dir_table, FileTableReader &file_table);
            
            void AddDirectory(BuildDirectoryContext **out, BuildDirectoryContext *parent_ctx, std::unique_ptr<BuildDirectoryContext> file_ctx);
            void AddFile(BuildDirectoryContext *parent_ctx, std::unique_ptr<BuildFileContext> file_ctx);
        public:
            explicit MergedTree(ncm::ProgramId program_id);
            ~MergedTree();

            void AddSdFiles();
            void AddStorageFiles(ams::fs::IStorage *storage, DataSourceType source_type);
    };

}
