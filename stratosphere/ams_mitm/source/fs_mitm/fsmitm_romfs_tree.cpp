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

        LayeredTree::LayeredTree(ncm::ProgramId program_id) : m_num_dirs(0), m_num_files(0), m_dir_table_size(0), m_file_table_size(0) {
            util::ConstructAt(m_heap_lock, program_id);

            auto res = m_directories.emplace(std::unique_ptr<BuildDirectoryContext>(CreateContext<BuildDirectoryContext>("", 0, nullptr)));
            AMS_ABORT_UNLESS(res.second);
            m_root = res.first->get();
            m_num_dirs = 1;
            m_dir_table_size = EntrySize<DirectoryEntry>(0);
        }

        LayeredTree::~LayeredTree() {
            util::DestroyAt(m_heap_lock);
        }

        void LayeredTree::AddDirectory(BuildDirectoryContext **out, std::unique_ptr<BuildDirectoryContext> child_ctx) {
            /* Check if the directory already exists. */
            auto existing = m_directories.find(child_ctx);
            if (existing != m_directories.end()) {
                *out = existing->get();
                return;
            }

            /* Add a new directory. */
            m_num_dirs++;
            m_dir_table_size += EntrySize<DirectoryEntry>(child_ctx->name_len);

            *out = child_ctx.get();
            m_directories.emplace(std::move(child_ctx));
        }

        void LayeredTree::AddFile(std::unique_ptr<BuildFileContext> file_ctx) {
            /* Check if the file already exists. */
            if (m_files.find(file_ctx) != m_files.end()) {
                return;
            }

            /* Add a new file. */
            m_num_files++;
            m_file_table_size += EntrySize<FileEntry>(file_ctx->name_len);
            m_files.emplace(std::move(file_ctx));
        }

    }

}
