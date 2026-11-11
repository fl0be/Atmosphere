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
#include "fsmitm_romfs_tree.hpp"
#include "fsmitm_romfs_format.hpp"

namespace ams::mitm::fs {

    using namespace ams::fs;

    namespace romfs {

        LayeredTree::LayeredTree(ncm::ProgramId program_id) {
            AcquireRomfsHeap(program_id);

            const auto res = m_directories.emplace(std::unique_ptr<BuildDirectoryContext>(CreateContext<BuildDirectoryContext>("", 0, nullptr)));
            AMS_ABORT_UNLESS(res.second);
            m_root = res.first->get();
        }

        LayeredTree::~LayeredTree() {
            ReleaseRomfsHeap();
        }

        BuildDirectoryContext *LayeredTree::AddDirectory(BuildDirectoryContext *parent_ctx, const char *name, size_t name_len) {
            std::unique_ptr<BuildDirectoryContext> child_ctx(CreateContext<BuildDirectoryContext>(name, name_len, parent_ctx));

            /* Check if the directory already exists. */
            auto existing = m_directories.find(child_ctx);
            if (existing != m_directories.end()) {
                return existing->get();
            }

            /* Add a new directory. */
            BuildDirectoryContext * const added = child_ctx.get();
            m_directories.emplace(std::move(child_ctx));
            return added;
        }

        void LayeredTree::AddFile(BuildDirectoryContext *parent_ctx, const char *name, size_t name_len, s64 size, s64 orig_offset, DataSourceType source_type) {
            std::unique_ptr<BuildFileContext> file_ctx(CreateContext<BuildFileContext>(name, name_len, parent_ctx, size, orig_offset, source_type));

            /* Check if the file already exists. */
            if (m_files.find(file_ctx) != m_files.end()) {
                return;
            }

            /* Add a new file. */
            m_files.emplace(std::move(file_ctx));
        }

    }

}
