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
#include "fsmitm_romfs_image.hpp"
#include "fsmitm_romfs_tree.hpp"
#include "fsmitm_romfs_builder.hpp"

namespace ams::mitm::fs {

    using namespace ams::fs;

    namespace romfs {

        Image::Image(std::unique_ptr<IStorage> s_r, std::unique_ptr<IStorage> f_r, ncm::ProgramId pr_id) : m_storage_romfs(std::move(s_r)), m_file_romfs(std::move(f_r)), m_program_id(std::move(pr_id)), m_header(), m_metadata(m_program_id) {
            /* ... */
        }

        Image::~Image() {
            for (size_t i = 0; i < m_source_infos.size(); i++) {
                m_source_infos[i].Cleanup();
            }
        }

        void Image::Initialize(bool walk_sd) {
            /* Build new virtual romfs. */
            MergedTree tree(m_program_id);

            if (walk_sd) {
                tree.AddSdFiles();
            }
            if (m_file_romfs) {
                tree.AddStorageFiles(m_file_romfs.get(), DataSourceType::File);
            }
            if (m_storage_romfs) {
                tree.AddStorageFiles(m_storage_romfs.get(), DataSourceType::Storage);
            }

            Builder builder(std::addressof(tree));
            builder.Build(std::addressof(m_source_infos), std::addressof(m_header), std::addressof(m_metadata));
        }

        Result Image::Read(s64 offset, void *buffer, size_t size) const {
            /* Validate offset/size. */
            const s64 virt_size = this->GetSize();
            R_UNLESS(offset >= 0,        fs::ResultInvalidOffset());
            R_UNLESS(offset < virt_size, fs::ResultInvalidOffset());
            if (static_cast<size_t>(virt_size - offset) < size) {
                size = static_cast<size_t>(virt_size - offset);
            }

            /* Find first source info via binary search. */
            auto it = std::lower_bound(m_source_infos.begin(), m_source_infos.end(), offset);
            u8 *cur_dst = static_cast<u8 *>(buffer);

            /* Our operator < compares against start of info instead of end, so we need to subtract one from lower bound. */
            it--;

            size_t read_so_far = 0;
            while (read_so_far < size) {
                const auto &cur_source = *it;
                AMS_ABORT_UNLESS(offset >= cur_source.virtual_offset);

                if (offset < cur_source.virtual_offset + cur_source.size) {
                    const s64 offset_within_source = offset - cur_source.virtual_offset;
                    const size_t cur_read_size = std::min(size - read_so_far, static_cast<size_t>(cur_source.size - offset_within_source));
                    switch (cur_source.source_type) {
                        case DataSourceType::Storage:
                            R_ABORT_UNLESS(m_storage_romfs->Read(cur_source.storage_source_info.offset + offset_within_source, cur_dst, cur_read_size));
                            break;
                        case DataSourceType::File:
                            R_ABORT_UNLESS(m_file_romfs->Read(cur_source.file_source_info.offset + offset_within_source, cur_dst, cur_read_size));
                            break;
                        case DataSourceType::LooseSdFile:
                            {
                                FsFile file;
                                R_ABORT_UNLESS(mitm::fs::OpenAtmosphereSdRomfsFile(std::addressof(file), m_program_id, cur_source.loose_source_info.path, OpenMode_Read));
                                ON_SCOPE_EXIT { fsFileClose(std::addressof(file)); };

                                u64 out_read = 0;
                                R_ABORT_UNLESS(fsFileRead(std::addressof(file), offset_within_source, cur_dst, cur_read_size, FsReadOption_None, std::addressof(out_read)));
                                AMS_ABORT_UNLESS(out_read == cur_read_size);
                            }
                            break;
                        case DataSourceType::Memory:
                            std::memcpy(cur_dst, cur_source.memory_source_info.data + offset_within_source, cur_read_size);
                            break;
                        case DataSourceType::Metadata:
                            cur_source.metadata_source_info.file->Read(offset_within_source, cur_dst, cur_read_size);
                            break;
                        AMS_UNREACHABLE_DEFAULT_CASE();
                    }
                    read_so_far += cur_read_size;
                    cur_dst     += cur_read_size;
                    offset      += cur_read_size;
                } else {
                    /* Explicitly handle padding. */
                    const auto &next_source = *(++it);
                    const size_t padding_size = static_cast<size_t>(next_source.virtual_offset - offset);

                    std::memset(cur_dst, 0, padding_size);
                    read_so_far += padding_size;
                    cur_dst     += padding_size;
                    offset      += padding_size;
                }
            }

            R_SUCCEED();
        }

    }

}
