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
#include <ranges>
#include "fsmitm_romfs_sources.hpp"
#include "fsmitm_romfs_format.hpp"

namespace ams::mitm::fs::romfs {

    struct BuildFileContext;

    template<typename Context>
    constexpr ALWAYS_INLINE size_t ContextAllocationSize(size_t name_len) {
        return sizeof(Context) + name_len + 1;
    }

    template<typename Context, typename... Args>
    Context *CreateContext(const char *name, size_t name_len, Args... args) {
        auto * const ctx = std::construct_at(AllocateTracked<Context>(Context::Allocation, ContextAllocationSize<Context>(name_len)), name_len, args...);
        std::memcpy(ctx->name, name, name_len);
        ctx->name[name_len] = '\x00';
        return ctx;
    }

    struct BuildDirectoryContext {
        NON_COPYABLE(BuildDirectoryContext);
        NON_MOVEABLE(BuildDirectoryContext);

        static constexpr AllocationType Allocation = AllocationType_BuildDirContext;

        BuildDirectoryContext * const parent;
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
        const u32 name_len;
        u32 entry_offset;
        char name[];

        BuildDirectoryContext(size_t len, BuildDirectoryContext *p) : parent(p), child(nullptr), sibling(nullptr), file(nullptr), name_len(static_cast<u32>(len)), entry_offset(0) { /* ... */ }

        void operator delete(BuildDirectoryContext *ctx, std::destroying_delete_t) {
            const size_t size = ContextAllocationSize<BuildDirectoryContext>(ctx->name_len);
            ctx->~BuildDirectoryContext();
            FreeTracked(Allocation, ctx, size);
        }

        size_t GetPathLength() const {
            if (this->parent == nullptr) {
                return 0;
            }

            return GetChildPathLength(this->parent->GetPathLength(), this->name_len);
        }

        size_t GetPath(char *dst) const {
            if (this->parent == nullptr) {
                dst[0] = '\x00';
                return 0;
            }

            return AppendChildPart(dst, this->parent->GetPath(dst), this->name, this->name_len);
        }
    };

    struct BuildFileContext {
        NON_COPYABLE(BuildFileContext);
        NON_MOVEABLE(BuildFileContext);

        static constexpr AllocationType Allocation = AllocationType_BuildFileContext;

        BuildDirectoryContext * const parent;
        union {
            BuildFileContext *sibling;
            u32 sibling_offset;
        };
        s64 offset;
        s64 size;
        s64 orig_offset;
        const u32 name_len;
        u32 entry_offset;
        u32 hash_value;
        DataSourceType source_type;
        char name[];

        BuildFileContext(size_t len, BuildDirectoryContext *p, s64 sz, s64 o_o, DataSourceType type) : parent(p), sibling(nullptr), offset(0), size(sz), orig_offset(o_o), name_len(static_cast<u32>(len)), entry_offset(0), hash_value(EmptyEntry), source_type(type) { /* ... */ }

        void operator delete(BuildFileContext *ctx, std::destroying_delete_t) {
            const size_t size = ContextAllocationSize<BuildFileContext>(ctx->name_len);
            ctx->~BuildFileContext();
            FreeTracked(Allocation, ctx, size);
        }

        size_t GetPath(char *dst) const {
            if (this->parent == nullptr) {
                dst[0] = '\x00';
                return 0;
            }

            return AppendChildPart(dst, this->parent->GetPath(dst), this->name, this->name_len);
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

    class LayeredTree {
        NON_COPYABLE(LayeredTree);
        NON_MOVEABLE(LayeredTree);
        private:
            template<typename T>
            struct Comparator {
                constexpr bool operator()(const std::unique_ptr<T> &lhs, const std::unique_ptr<T> &rhs) const {
                    char lhs_path[ams::fs::EntryNameLengthMax + 1];
                    char rhs_path[ams::fs::EntryNameLengthMax + 1];
                    const size_t lhs_len = lhs->GetPath(lhs_path);
                    const size_t rhs_len = rhs->GetPath(rhs_path);
                    return CompareNames(lhs_path, lhs_len, rhs_path, rhs_len) < 0;
                }
            };

            template<AllocationType AllocType, typename T>
            using ContextSet = std::set<std::unique_ptr<T>, Comparator<T>, TrackedAllocator<AllocType, std::unique_ptr<T>>>;

            template<typename Context, typename Set>
            static constexpr auto MakeView(Set &set) {
                return set | std::views::transform([](const auto &ctx) -> Context * { return ctx.get(); });
            }
        private:
            BuildDirectoryContext *m_root;
            ContextSet<AllocationType_DirContextSet, BuildDirectoryContext> m_directories;
            ContextSet<AllocationType_FileContextSet, BuildFileContext> m_files;
        public:
            explicit LayeredTree(ncm::ProgramId program_id);
            ~LayeredTree();

            BuildDirectoryContext *AddDirectory(BuildDirectoryContext *parent_ctx, const char *name, size_t name_len);
            void AddFile(BuildDirectoryContext *parent_ctx, const char *name, size_t name_len, s64 size, s64 orig_offset, DataSourceType source_type);

            BuildDirectoryContext *GetRoot() const { return m_root; }

            auto GetDirectories() { return MakeView<BuildDirectoryContext>(m_directories); }
            auto GetFiles() { return MakeView<BuildFileContext>(m_files); }

            BuildDirectoryContext *GetFrontDirectory() { return m_directories.empty() ? nullptr : m_directories.begin()->get(); }
            BuildFileContext *GetFrontFile() { return m_files.empty() ? nullptr : m_files.begin()->get(); }

            void RemoveFrontDirectory() {
                AMS_ABORT_UNLESS(!m_directories.empty());
                if (m_directories.begin()->get() == m_root) {
                    m_root = nullptr;
                }
                m_directories.erase(m_directories.begin());
            }

            void RemoveFrontFile() {
                AMS_ABORT_UNLESS(!m_files.empty());
                m_files.erase(m_files.begin());
            }
    };

}
