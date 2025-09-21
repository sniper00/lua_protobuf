#include "common/buffer.hpp"
#include "lua.hpp"
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>
#include <limits>
#include <cstdarg>

using namespace moon;

class lua_protobuf_error: public std::runtime_error {
public:
    using runtime_error::runtime_error;

    static lua_protobuf_error format(const char* fmt, ...) {
        va_list args;
        va_start(args, fmt);
        char buffer[1024];
        vsnprintf(buffer, sizeof(buffer), fmt, args);
        va_end(args);
        return lua_protobuf_error(buffer);
    }
};

enum class WireType : std::uint8_t {
    VARINT = 0, //int32/64, uint32/64, sint32/64, bool, enum
    FIXED64 = 1, //fixed64, sfixed64, double
    LENGTH_DELIMITED = 2, // string, bytes, nested messages, packed repeated fields
    SGROUP = 3, //deprecated
    EGROUP = 4, //deprecated
    FIXED32 = 5, //fixed32, sfixed32, float
    UNKNOWN = 99, //used for default setting in this library
};

enum class FieldType : std::uint8_t {
    TYPE_NONE = 0,
    TYPE_DOUBLE = 1, // double
    TYPE_FLOAT = 2, // float
    TYPE_INT64 = 3, // int64/sint64
    TYPE_UINT64 = 4, // uint64
    TYPE_INT32 = 5, // int32/sint32
    TYPE_FIXED64 = 6, // fixed64
    TYPE_FIXED32 = 7, // fixed32
    TYPE_BOOL = 8, // bool
    TYPE_STRING = 9, // string
    TYPE_GROUP = 10, // group (deprecated)
    TYPE_MESSAGE = 11, // message
    TYPE_BYTES = 12, // bytes
    TYPE_UINT32 = 13, // uint32
    TYPE_ENUM = 14, // enum
    TYPE_SFIXED32 = 15, // sfixed32
    TYPE_SFIXED64 = 16, // sfixed64
    TYPE_SINT32 = 17, // sint32
    TYPE_SINT64 = 18, // sint64
};

static WireType wiretype_by_fieldtype(FieldType t) {
    static constexpr WireType kFieldTypeToWireType[] = {
        WireType::EGROUP, // 0 (unused)
        WireType::FIXED64, // 1 TYPE_DOUBLE
        WireType::FIXED32, // 2 TYPE_FLOAT
        WireType::VARINT, // 3 TYPE_INT64
        WireType::VARINT, // 4 TYPE_UINT64
        WireType::VARINT, // 5 TYPE_INT32
        WireType::FIXED64, // 6 TYPE_FIXED64
        WireType::FIXED32, // 7 TYPE_FIXED32
        WireType::VARINT, // 8 TYPE_BOOL
        WireType::LENGTH_DELIMITED, // 9 TYPE_STRING
        WireType::LENGTH_DELIMITED, // 10 TYPE_GROUP (deprecated)
        WireType::LENGTH_DELIMITED, // 11 TYPE_MESSAGE
        WireType::LENGTH_DELIMITED, // 12 TYPE_BYTES
        WireType::VARINT, // 13 TYPE_UINT32
        WireType::VARINT, // 14 TYPE_ENUM
        WireType::FIXED32, // 15 TYPE_SFIXED32
        WireType::FIXED64, // 16 TYPE_SFIXED64
        WireType::VARINT, // 17 TYPE_SINT32
        WireType::VARINT, // 18 TYPE_SINT64
    };
    return kFieldTypeToWireType[static_cast<int>(t)];
}

constexpr size_t MIN_VARINT_LENGTH = 1;

/**
 * Maximum length (in bytes) of the varint encoding of a 64-bit value.
 */
constexpr size_t MAX_VARINT_LENGTH = 10;
constexpr size_t PB_MAX_RECURSION_DEPTH = 128;

constexpr std::byte operator"" _b(unsigned long long v) noexcept {
    return static_cast<std::byte>(v);
}

struct stream_reader {
    using value_type = buffer::value_type;
    using pointer = buffer::pointer;
    using const_pointer = buffer::const_pointer;

    stream_reader(const_pointer data, size_t size): data_(data), end_(data_ + size) {}

    const_pointer data() const {
        return data_;
    }

    const_pointer begin() const {
        return data_;
    }

    const_pointer end() const {
        return end_;
    }

    size_t size() const {
        return end_ - data_;
    }

    bool empty() const {
        return end_ == data_;
    }

    void consume_unchecked(size_t count) {
        assert(data_ + count <= end_);
        data_ += count;
    }

    bool consume(size_t count) {
        if (count > size())
            return false;
        data_ += count;
        return true;
    }

private:
    const_pointer data_;
    const_pointer end_;
};

using pb_variant =
    std::variant<std::monostate, lua_Number, lua_Integer, std::string_view, stream_reader>;

struct pb_enum {
    std::string name;
    std::unordered_map<std::string, int32_t> kvpair;
    std::unordered_map<int32_t, std::string> vkpair;
};

struct pb_message;
struct pb_field {
    bool packed = false;
    FieldType type = FieldType::TYPE_NONE;
    WireType wtype = WireType::UNKNOWN;
    int32_t oneof_index = -1;
    int32_t number = -1;
    int32_t label = -1;
    mutable const pb_message* message = nullptr;
    std::string name;
    std::string type_name;

    bool is_repeated() const {
        return label == 3;
    }
};

struct pb_message {
    bool is_map = false;
    std::string name;
    std::string meta_name;
    std::vector<pb_field> all_fields;
    std::vector<std::string> oneof_decl;

private:
    std::array<const pb_field*, 32> fast_fields {};
    std::unordered_map<int32_t, const pb_field*> fields;
    std::unordered_map<std::string_view, const pb_field*> sfields;

public:
    void init() {
        for (auto& field: all_fields) {
            fields.try_emplace(field.number, &field);
            sfields.try_emplace(field.name, &field);
        }
        for (const auto& [id, p]: fields) {
            if (id < (int)fast_fields.size()) {
                fast_fields[id] = p;
            }
        }
    }

    const pb_field* find_field_by_number(uint32_t field_num) const {
        if (field_num < fast_fields.size())
            return fast_fields[field_num];
        if (auto it = fields.find(field_num); it != fields.end())
            return it->second;
        return nullptr;
    }

    const pb_field* find_field(uint32_t tag) const {
        return find_field_by_number(tag >> 3);
    }

    const pb_field* find_field(std::string_view field_name) const {
        if (auto it = sfields.find(field_name); it != sfields.end())
            return it->second;
        return nullptr;
    }
};

struct pb_descriptor {
    bool ignore_empty = true;
    std::string syntax;
    std::deque<std::string> string_pool;
    std::vector<pb_message> all_messages;
    std::unordered_map<std::string_view, std::unique_ptr<const pb_enum>> enums;
    std::unordered_map<std::string_view, const pb_message*> messages;

    const pb_message* find_message(std::string_view name) const {
        if (auto it = messages.find(name); it != messages.end())
            return it->second;
        return nullptr;
    }
};

static const pb_descriptor* global_descriptor_ptr(const pb_descriptor* new_ptr = nullptr) {
    static std::atomic<const pb_descriptor*> descriptor_ptr { new pb_descriptor {} };
    if (new_ptr == nullptr) {
        return descriptor_ptr.load(std::memory_order_acquire);
    } else {
        descriptor_ptr.store(new_ptr, std::memory_order_release);
        return new_ptr;
    }
}

constexpr int pb_tag(uint32_t field_number, WireType wire_type) {
    return (field_number << 3) | (static_cast<uint32_t>(wire_type) & 7);
}

static std::string_view to_string_view(lua_State* L, int index) {
    size_t len;
    const char* str = lua_tolstring(L, index, &len);
    return std::string_view(str, len);
}

static std::string_view check_string_view(lua_State* L, int index) {
    size_t len;
    const char* str = luaL_checklstring(L, index, &len);
    return std::string_view(str, len);
}

struct protobuf {
    const pb_descriptor* descriptor = global_descriptor_ptr();

    using pb_trace_type = std::variant<const pb_message*, const pb_field*>;

    static std::vector<pb_trace_type>* get_pb_trace() {
        static thread_local std::vector<pb_trace_type> trace_back;
        trace_back.clear();
        return &trace_back;
    }

    std::string collect_trace(std::vector<pb_trace_type>* trace) const {
        std::string result;
        for (const auto& item: *trace) {
            if (std::holds_alternative<const pb_message*>(item)) {
                const pb_message* m = std::get<const pb_message*>(item);
                result += m->name;
            } else if (std::holds_alternative<const pb_field*>(item)) {
                const pb_field* f = std::get<const pb_field*>(item);
                result += ".";
                result += f->name;
            }
        }
        return result;
    }

    static inline bool is_packable(FieldType t) {
        // Packable if wire type is not LENGTH_DELIMITED, i.e., numeric/enum/bool
        switch (t) {
            case FieldType::TYPE_DOUBLE:
            case FieldType::TYPE_FLOAT:
            case FieldType::TYPE_INT64:
            case FieldType::TYPE_UINT64:
            case FieldType::TYPE_INT32:
            case FieldType::TYPE_FIXED64:
            case FieldType::TYPE_FIXED32:
            case FieldType::TYPE_BOOL:
            case FieldType::TYPE_UINT32:
            case FieldType::TYPE_ENUM:
            case FieldType::TYPE_SFIXED32:
            case FieldType::TYPE_SFIXED64:
            case FieldType::TYPE_SINT32:
            case FieldType::TYPE_SINT64:
                return true;
            default:
                return false;
        }
    }

    static inline bool is_allowed_map_key_type(FieldType t) {
        switch (t) {
            case FieldType::TYPE_STRING:
            case FieldType::TYPE_INT32:
            case FieldType::TYPE_INT64:
            case FieldType::TYPE_UINT32:
            case FieldType::TYPE_UINT64:
            case FieldType::TYPE_SINT32:
            case FieldType::TYPE_SINT64:
            case FieldType::TYPE_FIXED32:
            case FieldType::TYPE_FIXED64:
            case FieldType::TYPE_SFIXED32:
            case FieldType::TYPE_SFIXED64:
                return true;
            default:
                return false;
        }
    }

    const pb_message* find_message(std::string_view name) const {
        return descriptor->find_message(name);
    }

    const pb_enum* find_enum(const char* name) const {
        if (auto it = descriptor->enums.find(name); it != descriptor->enums.end())
            return it->second.get();
        return nullptr;
    }

    const pb_message* get_message(const pb_field* field) const {
        if (field->message)
            return field->message;
        if (field->type == FieldType::TYPE_MESSAGE && !field->type_name.empty()) {
            field->message = descriptor->find_message(field->type_name);
        }
        return field->message;
    }

    bool is_map(const pb_field* field) const {
        return get_message(field) && field->message->is_map;
    }

    static int64_t decode_sint(uint64_t val) {
        return static_cast<int64_t>((val >> 1) ^ -(val & 1));
    }

    static uint64_t encode_sint(int64_t val) {
        // Bit-twiddling magic stolen from the Google protocol buffer document;
        // val >> 63 is an arithmetic shift because val is signed
        auto uval = static_cast<uint64_t>(val);
        return (uval << 1) ^ (val >> 63);
    }

    template<typename T>
    static T read_varint(stream_reader& stream) {
        const int8_t* begin = reinterpret_cast<const int8_t*>(stream.begin());
        const int8_t* end = reinterpret_cast<const int8_t*>(stream.end());
        const int8_t* p = begin;
        uint64_t val = 0;

        // end is always greater than or equal to begin, so this subtraction is safe
        if (size_t(end - begin) >= MAX_VARINT_LENGTH) { // fast path
            int64_t b;
            do {
                b = *p++;
                val = (b & 0x7f);
                if (b >= 0) {
                    break;
                }
                b = *p++;
                val |= (b & 0x7f) << 7;
                if (b >= 0) {
                    break;
                }
                b = *p++;
                val |= (b & 0x7f) << 14;
                if (b >= 0) {
                    break;
                }
                b = *p++;
                val |= (b & 0x7f) << 21;
                if (b >= 0) {
                    break;
                }
                b = *p++;
                val |= (b & 0x7f) << 28;
                if (b >= 0) {
                    break;
                }
                b = *p++;
                val |= (b & 0x7f) << 35;
                if (b >= 0) {
                    break;
                }
                b = *p++;
                val |= (b & 0x7f) << 42;
                if (b >= 0) {
                    break;
                }
                b = *p++;
                val |= (b & 0x7f) << 49;
                if (b >= 0) {
                    break;
                }
                b = *p++;
                val |= (b & 0x7f) << 56;
                if (b >= 0) {
                    break;
                }
                b = *p++;
                val |= (b & 0x01) << 63;
                if (b >= 0) {
                    break;
                }
                throw lua_protobuf_error("Invalid varint value: too many bytes.");
            } while (false);
        } else {
            int shift = 0;
            while (p != end && *p < 0) {
                val |= static_cast<uint64_t>(*p++ & 0x7f) << shift;
                shift += 7;
            }
            if (p == end) {
                throw lua_protobuf_error("Invalid varint value: too few bytes.");
            }
            val |= static_cast<uint64_t>(*p++) << shift;
        }

        stream.consume_unchecked(p - begin);
        return (T)val;
    }

    template<typename T, std::enable_if_t<std::is_unsigned_v<T>, int> = 0>
    inline static size_t write_varint(uint8_t* data, T val) {
        uint8_t* p = data;
        while (val >= 0x80U) {
            *p++ = 0x80 | (val & 0x7f);
            val >>= 7;
        }
        *p++ = uint8_t(val);
        return size_t(p - data);
    }

    template<typename T, std::enable_if_t<std::is_unsigned_v<T>, int> = 0>
    static size_t write_varint(buffer& buf, T val) {
        auto [data, _] = buf.prepare(MAX_VARINT_LENGTH);
        auto len = write_varint(reinterpret_cast<uint8_t*>(data), val);
        buf.commit_unchecked(len);
        return len;
    }

    template<typename T>
    static T read_fixed(stream_reader& stream) {
        if (stream.size() < sizeof(T))
            throw lua_protobuf_error("read_fixed buffer length not enough");

        T v {};
        auto data = stream.data();
        memcpy(&v, data, sizeof(T));
        stream.consume_unchecked(sizeof(T));
        return v;
    }

    template<typename T>
    void write_fixed(buffer& buf, T value) const {
        buf.write_back({ (const char*)&value, sizeof(T) });
    }

    static std::string_view read_string(stream_reader& stream) {
        uint32_t length = read_varint<uint32_t>(stream);

        if (length > stream.size()) {
            throw lua_protobuf_error(
                "read_string: buffer length not enough, need " + std::to_string(length)
                + " bytes, have " + std::to_string(stream.size())
            );
        }

        auto data = stream.data();
        stream.consume_unchecked(length);
        return std::string_view { data, length };
    }

    static void write_string(buffer& buf, std::string_view value) {
        size_t length = value.size();
        write_varint(buf, (uint32_t)length);
        buf.write_back({ value.data(), length });
    }

    static stream_reader read_len_prefixed(stream_reader& stream) {
        uint32_t length = read_varint<uint32_t>(stream);
        // Check if we have enough data
        if (length > stream.size()) {
            throw lua_protobuf_error(
                "read_len_prefixed: buffer length not enough, need " + std::to_string(length)
                + " bytes, have " + std::to_string(stream.size())
            );
        }

        // Zero-copy extraction of the length-prefixed data
        auto data = stream.data();
        stream.consume_unchecked(length);
        return stream_reader { data, length };
    }

    static void write_len_prefixed(buffer& buf, const stream_reader& stream) {
        size_t length = stream.size();
        if (length < 0x80U) {
            buf.write_back(static_cast<uint8_t>(length));
            //assert(memcmp(buf.data() + buf.size(), stream.data(), length) == 0);
            buf.commit_unchecked(length);
        } else {
            std::array<uint8_t, 16> tmp;
            size_t size = write_varint(tmp.data(), length);
            auto base = buf.size();
            auto data = buf.data();
            buf.commit_unchecked(length + MIN_VARINT_LENGTH);
            buf.prepare(size);
            // Make space for the varint length prefix
            buf.shift_data(base + MIN_VARINT_LENGTH, length, base + size);
            // Copy the length prefix
            for (size_t i = 0; i < size; ++i) {
                data[base + i] = tmp[i];
            }
            buf.commit_unchecked(size - MIN_VARINT_LENGTH);
        }
    }

    static void write_wire_type(buffer& buf, int field_num, WireType wtype) {
        uint32_t value = (field_num << 3) | (((uint32_t)wtype) & 7);
        write_varint(buf, value);
    }

    static void skip_field(stream_reader& stream, uint32_t field_tag) {
        if (stream.empty()) {
            throw lua_protobuf_error("skip_field: empty stream");
        }

        auto wire_type = static_cast<WireType>(field_tag & 0x07);

        // Handle each wire type appropriately
        switch (wire_type) {
            case WireType::VARINT: {
                const auto* begin = reinterpret_cast<const int8_t*>(stream.begin());
                const auto* iend = reinterpret_cast<const int8_t*>(stream.end());
                const int8_t* p = begin;

                while (p != iend && *p < 0) {
                    ++p;
                }

                if (size_t(p - begin) >= MAX_VARINT_LENGTH) {
                    throw lua_protobuf_error("skip_field: varint_too_long_exception");
                }

                if (p == iend) {
                    throw lua_protobuf_error("skip_field: end_of_buffer_exception");
                }

                ++p;

                stream.consume_unchecked(p - begin);
                break;
            }
            case WireType::FIXED64:
                if (stream.size() < sizeof(int64_t)) {
                    throw lua_protobuf_error("skip_field: not enough data for fixed64");
                }
                stream.consume_unchecked(sizeof(int64_t));
                break;

            case WireType::FIXED32:
                if (stream.size() < sizeof(int32_t)) {
                    throw lua_protobuf_error("skip_field: not enough data for fixed32");
                }
                stream.consume_unchecked(sizeof(int32_t));
                break;

            case WireType::LENGTH_DELIMITED: {
                read_len_prefixed(stream);
                break;
            }

            default:
                throw lua_protobuf_error(
                    "skip_field: invalid wire type " + std::to_string(static_cast<int>(wire_type))
                );
        }
    }

    bool is_field_value_empty(const pb_variant& val) const {
        if (auto* pv = std::get_if<lua_Integer>(&val))
            return *pv == 0;
        if (auto* pv = std::get_if<std::string_view>(&val))
            return pv->empty(); // Use empty() instead of size() == 0
        if (auto* pv = std::get_if<lua_Number>(&val))
            return *pv == 0;
        if (auto* pv = std::get_if<stream_reader>(&val))
            return pv->size() == 0; // treat empty nested message as empty when ignore_empty is true
        return false;
    }

    void fill_message(lua_State* L, const pb_message* msg) const {
        lua_createtable(L, 0, (int)msg->all_fields.size());
        for (auto& field: msg->all_fields) {
            auto bmap = is_map(&field);
            auto brepeated = field.is_repeated();
            if (brepeated || bmap) {
                lua_createtable(L, brepeated ? 4 : 0, bmap ? 4 : 0);
                lua_setfield(L, -2, field.name.data());
                continue;
            }
        }

        if (luaL_newmetatable(L, msg->meta_name.data())) {
            lua_createtable(L, 0, (int)msg->all_fields.size());
            for (auto& field: msg->all_fields) {
                if (is_map(&field) || field.is_repeated())
                    continue;
                switch (field.type) {
                    case FieldType::TYPE_BOOL:
                        lua_pushboolean(L, 0);
                        break;
                    case FieldType::TYPE_DOUBLE:
                        lua_pushnumber(L, 0);
                        break;
                    case FieldType::TYPE_FLOAT:
                        lua_pushnumber(L, 0);
                        break;
                    case FieldType::TYPE_BYTES:
                        lua_pushstring(L, "");
                        break;
                    case FieldType::TYPE_STRING:
                        lua_pushstring(L, "");
                        break;
                    case FieldType::TYPE_MESSAGE:
                        lua_pushnil(L);
                        break;
                    default:
                        lua_pushinteger(L, 0);
                        break;
                }
                lua_setfield(L, -2, field.name.data());
            }
            lua_setfield(L, -2, "__index");
        }
        lua_setmetatable(L, -2);
    }

    void decode_field(lua_State* L, stream_reader& stream, const pb_field* field, std::vector<pb_trace_type>* trace, int depth) {
        switch (field->type) {
            case FieldType::TYPE_FLOAT:
                lua_pushnumber(L, read_fixed<float>(stream));
                break;
            case FieldType::TYPE_DOUBLE:
                lua_pushnumber(L, read_fixed<double>(stream));
                break;
            case FieldType::TYPE_FIXED32:
                lua_pushinteger(L, read_fixed<uint32_t>(stream));
                break;
            case FieldType::TYPE_FIXED64:
                lua_pushinteger(L, read_fixed<uint64_t>(stream));
                break;
            case FieldType::TYPE_BOOL:
                lua_pushboolean(L, read_varint<uint32_t>(stream));
                break;
            case FieldType::TYPE_INT32:
                lua_pushinteger(L, static_cast<int32_t>(read_varint<uint32_t>(stream)));
                break;
            case FieldType::TYPE_INT64:
                lua_pushinteger(L, read_varint<int64_t>(stream));
                break;
            case FieldType::TYPE_UINT32:
                lua_pushinteger(L, read_varint<uint32_t>(stream));
                break;
            case FieldType::TYPE_UINT64:
                lua_pushinteger(L, read_varint<uint64_t>(stream));
                break;
            case FieldType::TYPE_SINT32:
                lua_pushinteger(L, decode_sint(read_varint<uint32_t>(stream)));
                break;
            case FieldType::TYPE_SINT64:
                lua_pushinteger(L, decode_sint(read_varint<uint64_t>(stream)));
                break;
            case FieldType::TYPE_SFIXED32:
                lua_pushinteger(L, read_fixed<int32_t>(stream));
                break;
            case FieldType::TYPE_SFIXED64:
                lua_pushinteger(L, read_fixed<int64_t>(stream));
                break;
            case FieldType::TYPE_ENUM:
                lua_pushinteger(L, read_varint<uint32_t>(stream));
                break;
            case FieldType::TYPE_MESSAGE: {
                auto s = read_len_prefixed(stream);
                decode_message(L, s, get_message(field), trace, depth + 1);
                break;
            }
            case FieldType::TYPE_BYTES:
            case FieldType::TYPE_STRING: {
                auto s = read_string(stream);
                lua_pushlstring(L, s.data(), s.size());
                break;
            }
            default:
                throw lua_protobuf_error::format("decode_field invalid FieldType at %s", collect_trace(trace).data());
        }
    }

    void decode_map(lua_State* L, stream_reader& stream, const pb_field* field, std::vector<pb_trace_type>* trace, int depth) {
        lua_getfield(L, -1, field->name.data());
        auto msg = get_message(field);
        if (!msg) {
            throw lua_protobuf_error::format("decode_map: no message type for field %s at %s", field->name.c_str(), collect_trace(trace).data());
        }

        auto s = read_len_prefixed(stream);
        // Maps in protobuf are represented as key-value pairs
        // We'll store the key temporarily until we get the value
        bool has_key = false;

        while (!s.empty()) {
            uint32_t tag = read_varint<uint32_t>(s);
            const pb_field* kvfield = msg->find_field(tag);
            if (!kvfield) {
                skip_field(s, tag);
                continue;
            }

            decode_field(L, s, kvfield, trace, depth);

            // When we get the value (field number 2), perform the map set with the stored key
            if (kvfield->number == 1) {
                has_key = true;
            } else if (kvfield->number == 2 && has_key) {
                lua_rawset(L, -3);
                has_key = false;
            } else {
                // Unexpected field in map entry, discard it
                lua_pop(L, 1);
            }
        }

        // If we have a dangling key without a value, remove it
        if (has_key) {
            lua_pop(L, 1);
        }

        lua_pop(L, 1); // Pop the map table
    }

    void decode_repeated(lua_State* L, stream_reader& stream, const pb_field* field, bool is_packed_wire, std::vector<pb_trace_type>* trace, int depth) {
        lua_getfield(L, -1, field->name.data());
        bool packable = is_packable(field->type);
        if (is_packed_wire && packable) {
            int len = 0;
            auto s = read_len_prefixed(stream);
            while (!s.empty()) {
                decode_field(L, s, field, trace, depth);
                lua_rawseti(L, -2, ++len);
            }
        } else {
            // Handle regular (non-packed) repeated field
            auto base_len = (int)lua_rawlen(L, -1);
            decode_field(L, stream, field, trace, depth);
            lua_rawseti(L, -2, base_len + 1);
        }
        lua_pop(L, 1); // Pop the repeated field table
    }

    void decode_message(lua_State* L, stream_reader& stream, const pb_message* msg, std::vector<pb_trace_type>* trace, int depth) {
        if (!msg) {
            throw lua_protobuf_error("decode_message: null message descriptor");
        }
        if (depth >= (int)PB_MAX_RECURSION_DEPTH) {
            auto path = collect_trace(trace);
            throw lua_protobuf_error::format("decode_message: maximum recursion depth exceeded at %s", path.data());
        }
        trace->emplace_back(msg);
        fill_message(L, msg);
        while (!stream.empty()) {
            uint32_t tag = read_varint<uint32_t>(stream);
            WireType wire_type = static_cast<WireType>(tag & 0x07);
            if ((tag >> 3) == 0) {
                auto path = collect_trace(trace);
                throw lua_protobuf_error::format("decode_message: invalid tag (field_number=0) at %s", path.data());
            }
            const pb_field* field = msg->find_field(tag);
            if (!field) {
                skip_field(stream, tag);
                continue;
            }
            trace->emplace_back(field);
            if (is_map(field)) [[unlikely]] {
                if (wire_type != WireType::LENGTH_DELIMITED) {
                    auto path = collect_trace(trace);
                    throw lua_protobuf_error::format("decode_map: invalid wire type for map field at %s", path.data());
                }
                decode_map(L, stream, field, trace, depth + 1);
                trace->pop_back();
                continue;
            }
            if (field->is_repeated()) [[unlikely]] {
                bool is_packed_wire = (wire_type == WireType::LENGTH_DELIMITED);
                decode_repeated(L, stream, field, is_packed_wire, trace, depth);
                trace->pop_back();
                continue;
            }
            if (wire_type != field->wtype) {
                auto path = collect_trace(trace);
                throw lua_protobuf_error::format("decode_field: wire type mismatch at %s", path.data());
            }
            decode_field(L, stream, field, trace, depth);
            //oneof field handling
            if (field->oneof_index < 0) {
                lua_setfield(L, -2, field->name.data());
            } else {
                // For oneof fields, we need to store both the field value and the field name in the oneof_decl
                if (field->oneof_index < static_cast<int32_t>(msg->oneof_decl.size())) {
                    lua_setfield(L, -2, field->name.data());
                    lua_pushstring(L, field->name.data());
                    lua_setfield(L, -2, msg->oneof_decl[field->oneof_index].data());
                } else {
                    // Invalid oneof_index
                    lua_setfield(L, -2, field->name.data());
                }
            }
            trace->pop_back();
        }
        trace->pop_back();
    }

    static size_t buffer_reserve_varint_space(buffer& buf) {
        buf.prepare(64);
        buf.commit_unchecked(MIN_VARINT_LENGTH); //reserve 1 byte for optimaze varint length
        return buf.size();
    }

    static stream_reader buffer_revert_varint_space(buffer& buf, size_t origin_size) {
        auto data = buf.data() + origin_size; // message begin data
        auto len = buf.size() - origin_size; // message length
        buf.revert(len + MIN_VARINT_LENGTH); // revert to original write position
        return stream_reader { data, len };
    }

    pb_variant get_field_value(lua_State* L, buffer& buf, const pb_field* field, int index, std::vector<pb_trace_type>* trace, int depth) {
        switch (field->type) {
            case FieldType::TYPE_ENUM:
                return lua_tointeger(L, index);
            case FieldType::TYPE_FLOAT:
                return lua_tonumber(L, index);
            case FieldType::TYPE_DOUBLE:
                return lua_tonumber(L, index);
            case FieldType::TYPE_FIXED32:
                {
                    auto v = lua_tointeger(L, index);
                    if (v < 0 || v > std::numeric_limits<uint32_t>::max())
                        throw lua_protobuf_error::format("encode: fixed32 out of range at %s", collect_trace(trace).data());
                    return v;
                }
            case FieldType::TYPE_FIXED64:
                {
                    auto v = lua_tointeger(L, index);
                    if (v < 0)
                        throw lua_protobuf_error::format("encode: fixed64 must be non-negative at %s", collect_trace(trace).data());
                    return v;
                }
            case FieldType::TYPE_INT32:
                {
                    auto v = lua_tointeger(L, index);
                    if (v < std::numeric_limits<int32_t>::min()
                        || v > std::numeric_limits<int32_t>::max())
                        throw lua_protobuf_error::format("encode: int32 out of range at %s", collect_trace(trace).data());
                    return v;
                }
            case FieldType::TYPE_INT64:
                return lua_tointeger(L, index);
            case FieldType::TYPE_UINT32:
                {
                    auto v = lua_tointeger(L, index);
                    if (v < 0 || v > std::numeric_limits<uint32_t>::max())
                        throw lua_protobuf_error::format("encode: uint32 out of range at %s", collect_trace(trace).data());
                    return v;
                }
            case FieldType::TYPE_UINT64:
                {
                    auto v = lua_tointeger(L, index);
                    if (v < 0)
                        throw lua_protobuf_error::format("encode: uint64 must be non-negative at %s", collect_trace(trace).data());
                    return v;
                }
            case FieldType::TYPE_SINT32:
                {
                    auto v = lua_tointeger(L, index);
                    if (v < std::numeric_limits<int32_t>::min()
                        || v > std::numeric_limits<int32_t>::max())
                        throw lua_protobuf_error::format("encode: sint32 out of range at %s", collect_trace(trace).data());
                    return v;
                }
            case FieldType::TYPE_SINT64:
                return lua_tointeger(L, index);
            case FieldType::TYPE_SFIXED32:
                {
                    auto v = lua_tointeger(L, index);
                    if (v < std::numeric_limits<int32_t>::min()
                        || v > std::numeric_limits<int32_t>::max())
                        throw lua_protobuf_error::format("encode: sfixed32 out of range at %s", collect_trace(trace).data());
                    return v;
                }
            case FieldType::TYPE_SFIXED64:
                return lua_tointeger(L, index);
            case FieldType::TYPE_BOOL:
                return (lua_Integer)lua_toboolean(L, index);
            case FieldType::TYPE_BYTES:
            case FieldType::TYPE_STRING: {
                if (lua_type(L, index) != LUA_TSTRING) {
                    throw lua_protobuf_error::format(
                        "expected 'string' type got '%s' type for: '%s'",
                        lua_typename(L, lua_type(L, index)),
                        collect_trace(trace).data()
                    );
                }
                size_t len;
                auto s = lua_tolstring(L, index, &len);
                return std::string_view(s, len);
            }
            case FieldType::TYPE_MESSAGE: {
                auto base = buffer_reserve_varint_space(buf);
                encode_message(L, buf, get_message(field), field->type_name, trace, depth + 1);
                return buffer_revert_varint_space(buf, base);
            }
            default:
                return std::monostate();
        }
    }

    void encode_field(buffer& buf, const pb_field* field, pb_variant val) const {
        switch (field->type) {
            case FieldType::TYPE_FLOAT:
                write_fixed<float>(buf, std::get<lua_Number>(val));
                break;
            case FieldType::TYPE_DOUBLE:
                write_fixed<double>(buf, std::get<lua_Number>(val));
                break;
            case FieldType::TYPE_FIXED32:
                write_fixed<uint32_t>(buf, std::get<lua_Integer>(val));
                break;
            case FieldType::TYPE_FIXED64:
                write_fixed<uint64_t>(buf, std::get<lua_Integer>(val));
                break;
            case FieldType::TYPE_BOOL:
                write_varint<uint8_t>(buf, std::get<lua_Integer>(val));
                break;
            case FieldType::TYPE_INT32:
                write_varint<uint32_t>(buf, static_cast<uint32_t>(std::get<lua_Integer>(val)));
                break;
            case FieldType::TYPE_INT64:
                write_varint<uint64_t>(buf, std::get<lua_Integer>(val));
                break;
            case FieldType::TYPE_UINT32:
                write_varint<uint32_t>(buf, std::get<lua_Integer>(val));
                break;
            case FieldType::TYPE_UINT64:
                write_varint<uint64_t>(buf, std::get<lua_Integer>(val));
                break;
            case FieldType::TYPE_SINT32:
                write_varint<uint32_t>(buf, encode_sint(std::get<lua_Integer>(val)));
                break;
            case FieldType::TYPE_SINT64:
                write_varint<uint64_t>(buf, encode_sint(std::get<lua_Integer>(val)));
                break;
            case FieldType::TYPE_SFIXED32:
                write_fixed<int32_t>(buf, static_cast<int32_t>(std::get<lua_Integer>(val)));
                break;
            case FieldType::TYPE_SFIXED64:
                write_fixed<int64_t>(buf, static_cast<int64_t>(std::get<lua_Integer>(val)));
                break;
            case FieldType::TYPE_ENUM:
                write_varint<uint32_t>(buf, std::get<lua_Integer>(val));
                break;
            case FieldType::TYPE_BYTES:
                write_string(buf, std::get<std::string_view>(val));
                break;
            case FieldType::TYPE_STRING:
                write_string(buf, std::get<std::string_view>(val));
                break;
            case FieldType::TYPE_MESSAGE:
                write_len_prefixed(buf, std::get<stream_reader>(val));
                break;
            default:
                throw lua_protobuf_error(
                    "encode failed: " + field->name + " use unsupported field type!"
                );
        }
    }

    void encode_map(lua_State* L, buffer& buf, const pb_field* field, int index, std::vector<pb_trace_type>* trace, int depth) {
        auto message = get_message(field);
        if (!message) {
            throw lua_protobuf_error::format("encode_map: no message type for field %s at %s", field->name.c_str(), collect_trace(trace).data());
        }
        const pb_field* kfield = message->find_field_by_number(1);
        const pb_field* vfield = message->find_field_by_number(2);
        index = lua_absindex(L, index);
        lua_pushnil(L);
        while (lua_next(L, index)) {
            write_wire_type(buf, field->number, field->wtype);
            auto base = buffer_reserve_varint_space(buf);

            write_wire_type(buf, kfield->number, kfield->wtype);
            trace->emplace_back(kfield);
            encode_field(buf, kfield, get_field_value(L, buf, kfield, -2, trace, depth));
            trace->pop_back();

            write_wire_type(buf, vfield->number, vfield->wtype);
            trace->emplace_back(vfield);
            encode_field(buf, vfield, get_field_value(L, buf, vfield, -1, trace, depth));
            trace->pop_back();

            stream_reader stream = buffer_revert_varint_space(buf, base);
            write_len_prefixed(buf, stream);
            lua_pop(L, 1);
        }
    }

    void encode_repeated(lua_State* L, buffer& buf, const pb_field* field, int index, std::vector<pb_trace_type>* trace, int depth) {
        auto rawlen = lua_rawlen(L, index);
        if (rawlen == 0 && descriptor->ignore_empty)
            return;
        if (field->packed) {
            write_wire_type(buf, field->number, WireType::LENGTH_DELIMITED);
            size_t base = buffer_reserve_varint_space(buf);
            for (int i = 1; i <= (int)rawlen; ++i) {
                lua_geti(L, index, i);
                encode_field(buf, field, get_field_value(L, buf, field, -1, trace, depth));
                lua_pop(L, 1);
            }
            stream_reader stream = buffer_revert_varint_space(buf, base);
            write_len_prefixed(buf, stream);
        } else {
            for (int i = 1; i <= (int)rawlen; ++i) {
                lua_geti(L, index, i);
                write_wire_type(buf, field->number, field->wtype);
                encode_field(buf, field, get_field_value(L, buf, field, -1, trace, depth));
                lua_pop(L, 1);
            }
        }
    }

    void encode_message(lua_State* L, buffer& buf, const pb_message* msg, std::string_view name, std::vector<pb_trace_type>* trace, int depth) {
        if (nullptr == msg) {
            throw lua_protobuf_error("encode_message: can not found message " + std::string(name));
        }
        if (depth >= (int)PB_MAX_RECURSION_DEPTH) {
            throw lua_protobuf_error::format("encode_message: maximum recursion depth exceeded at %s", collect_trace(trace).data());
        }
        trace->emplace_back(msg);
        lua_pushnil(L);
        bool oneofencode = false;
        while (lua_next(L, -2) != 0) {
            if (lua_type(L, -2) == LUA_TSTRING) {
                const pb_field* field = msg->find_field(to_string_view(L, -2));
                if (field) {
                    trace->emplace_back(field);
                    if (is_map(field)) {
                        encode_map(L, buf, field, -1, trace, depth + 1);
                    } else if (field->is_repeated()) {
                        encode_repeated(L, buf, field, -1, trace, depth);
                    } else {
                        if (field->oneof_index >= 0) {
                            if (oneofencode) {
                                lua_pop(L, 1);
                                throw lua_protobuf_error::format("encode_message: multiple oneof fields set at %s", collect_trace(trace).data());
                            }
                            oneofencode = true;
                        }

                        size_t origin_size = buf.size();
                        write_wire_type(buf, field->number, field->wtype);
                        auto val = get_field_value(L, buf, field, -1, trace, depth);
                        if (!(is_field_value_empty(val) && descriptor->ignore_empty)) {
                            encode_field(buf, field, val);
                        } else {
                            buf.revert(buf.size() - origin_size); // revert to origin size
                        }
                    }
                    trace->pop_back();
                }
            }
            lua_pop(L, 1);
        }
        trace->pop_back();
    }


    static void read_enum_value(stream_reader& stream, pb_enum* info) {
        int32_t value = 0;
        auto s = read_len_prefixed(stream);
        while (!s.empty()) {
            uint32_t tag = read_varint<uint32_t>(s);
            switch (tag) {
                case pb_tag(1, WireType::LENGTH_DELIMITED):
                    info->name = read_string(s);
                    break;
                case pb_tag(2, WireType::VARINT):
                    value = read_varint<int32_t>(s);
                    break;
                default:
                    skip_field(s, tag);
                    break;
            }
        }
        info->kvpair.try_emplace(info->name, value);
        info->vkpair.try_emplace(value, info->name);
    }

    static void read_enum(stream_reader& stream, std::string package, pb_descriptor* descriptor) {
        auto penum = std::make_unique<pb_enum>();
        auto s = read_len_prefixed(stream);
        while (!s.empty()) {
            uint32_t tag = read_varint<uint32_t>(s);
            switch (tag) {
                case pb_tag(1, WireType::LENGTH_DELIMITED):
                    penum->name = read_string(s), package += "." + penum->name;
                    break;
                case pb_tag(2, WireType::LENGTH_DELIMITED):
                    read_enum_value(s, penum.get());
                    break;
                default:
                    skip_field(s, tag);
                    break;
            }
        }
        descriptor->enums.try_emplace(package, std::move(penum));
    }

    static void read_field_option(stream_reader& stream, pb_field* field) {
        auto s = read_len_prefixed(stream);
        while (!s.empty()) {
            if (auto tag = read_varint<uint32_t>(s); tag == pb_tag(2, WireType::VARINT)) {
                field->packed = read_varint<uint32_t>(s);
            } else {
                skip_field(s, tag);
            }
        }
    }

    static void read_message_option(stream_reader& stream, pb_message* msg) {
        auto s = read_len_prefixed(stream);
        while (!s.empty()) {
            if (auto tag = read_varint<uint32_t>(s); tag == pb_tag(7, WireType::VARINT)) {
                msg->is_map = read_varint<uint32_t>(s);
            } else {
                skip_field(s, tag);
            }
        }
    }

    static void read_oneof(stream_reader& stream, pb_message* msg) {
        auto s = read_len_prefixed(stream);
        while (!s.empty()) {
            if (auto tag = read_varint<uint32_t>(s); tag == pb_tag(1, WireType::LENGTH_DELIMITED)) {
                msg->oneof_decl.emplace_back(read_string(s));
            } else {
                skip_field(s, tag);
            }
        }
    }

    static void
    read_field(stream_reader& stream, pb_message* msg, const pb_descriptor* descriptor) {
        pb_field field;
        auto s = read_len_prefixed(stream);
        while (!s.empty()) {
            uint32_t tag = read_varint<uint32_t>(s);
            switch (tag) {
                case pb_tag(1, WireType::LENGTH_DELIMITED):
                    field.name = read_string(s);
                    break;
                case pb_tag(3, WireType::VARINT):
                    field.number = read_varint<int32_t>(s);
                    break;
                case pb_tag(4, WireType::VARINT):
                    field.label = read_varint<int32_t>(s);
                    break;
                case pb_tag(5, WireType::VARINT):
                    field.type = (FieldType)read_varint<int32_t>(s);
                    break;
                case pb_tag(6, WireType::LENGTH_DELIMITED):
                    field.type_name = read_string(s).substr(1);
                    break;
                case pb_tag(9, WireType::VARINT):
                    field.oneof_index = read_varint<int32_t>(s);
                    break;
                case pb_tag(8, WireType::LENGTH_DELIMITED):
                    read_field_option(s, &field);
                    break;
                default:
                    skip_field(s, tag);
                    break;
            }
        }
        field.wtype = wiretype_by_fieldtype(field.type);
        //Only repeated fields of primitive numeric types (types which use the varint, 32-bit, or 64-bit wire types) can be declared as packed.
        field.packed = (field.wtype != WireType::LENGTH_DELIMITED)
            && (field.packed || (field.label == 3 && descriptor->syntax == "proto3"));

        msg->all_fields.emplace_back(std::move(field));

        // auto [res, ok] = msg->fields.try_emplace(field.number, field);
        // msg->sfields.try_emplace(res->second.name, &res->second);
    }

    static void
    read_message(stream_reader& stream, std::string package, pb_descriptor* descriptor) {
        pb_message message {};
        auto s = read_len_prefixed(stream);
        while (!s.empty()) {
            uint32_t tag = read_varint<uint32_t>(s);
            switch (tag) {
                case pb_tag(1, WireType::LENGTH_DELIMITED):
                    message.name = read_string(s);
                    if(package.empty()) {
                        package = message.name;
                    } else {
                        package += "." + message.name;
                    }
                    break;
                case pb_tag(2, WireType::LENGTH_DELIMITED):
                    read_field(s, &message, descriptor);
                    break;
                case pb_tag(3, WireType::LENGTH_DELIMITED):
                    read_message(s, package, descriptor);
                    break;
                case pb_tag(4, WireType::LENGTH_DELIMITED):
                    read_enum(s, package, descriptor);
                    break;
                case pb_tag(8, WireType::LENGTH_DELIMITED):
                    read_oneof(s, &message);
                    break;
                case pb_tag(7, WireType::LENGTH_DELIMITED):
                    read_message_option(s, &message);
                    break;
                default:
                    skip_field(s, tag);
                    break;
            }
        }

        message.meta_name = "__protobuf_meta_" + package;
        descriptor->all_messages.push_back(std::move(message));
    }

    static void read_file_descriptor(stream_reader& stream, pb_descriptor* descriptor) {
        std::string package;
        auto s = read_len_prefixed(stream);
        while (!s.empty()) {
            uint32_t tag = read_varint<uint32_t>(s);
            switch (tag) {
                case pb_tag(2, WireType::LENGTH_DELIMITED):
                    package = read_string(s);
                    break;
                case pb_tag(4, WireType::LENGTH_DELIMITED):
                    read_message(s, package, descriptor);
                    break;
                case pb_tag(5, WireType::LENGTH_DELIMITED):
                    read_enum(s, package, descriptor);
                    break;
                case pb_tag(12, WireType::LENGTH_DELIMITED):
                    descriptor->syntax = read_string(s);
                    break;
                default:
                    skip_field(s, tag);
                    break;
            }
        }
    }

    static void read_file_descriptor_set(stream_reader& stream, pb_descriptor* descriptor) {
        while (!stream.empty()) {
            if (auto tag = read_varint<uint32_t>(stream);
                tag == pb_tag(1, WireType::LENGTH_DELIMITED))
            {
                read_file_descriptor(stream, descriptor);
            } else {
                // Skip any other tags that are not part of the file descriptor set
                skip_field(stream, tag);
            }
        }

        protobuf pb{};
        // After reading all messages, populate the maps for quick lookup
        for (auto& message: descriptor->all_messages) {
            for(auto& field : message.all_fields) {
                if (pb.is_map(&field)) {
                    if(auto m = pb.get_message(&field); m == nullptr) {
                        throw lua_protobuf_error("encode_map: no message type for field " + field.name);
                    } else {
                        if(m->all_fields.size() != 2){
                            throw lua_protobuf_error("encode_map: invalid message type for field " + field.name);
                        }
                        const pb_field* kf = m->find_field_by_number(1);
                        if (!is_allowed_map_key_type(kf->type)) {
                            throw lua_protobuf_error("invalid map key type in field " + field.name);
                        }
                    }
                } else if (field.is_repeated()) {
                    
                }
            }

            message.init();
            descriptor->messages.try_emplace(
                std::string_view { message.meta_name }.substr(16),
                &message
            );
        }
    }
};

int load_pb(lua_State* L) {
    try {
        auto descriptor = std::make_unique<pb_descriptor>();
        size_t len;
        auto data = lua_tolstring(L, 1, &len);
        stream_reader stream { data, len };
        protobuf::read_file_descriptor_set(stream, descriptor.get());
        global_descriptor_ptr(descriptor.release());
        lua_pushboolean(L, 1);
    } catch (const lua_protobuf_error& e) {
        luaL_error(L, e.what());
    }
    return 1;
}

static buffer* get_thread_encode_buffer() {
    static thread_local buffer thread_encode_buffer { 64 * 1024 };
    thread_encode_buffer.clear();
    return &thread_encode_buffer;
}

int pb_encode(lua_State* L) {
    auto cmd_name = luaL_checkstring(L, 1);
    luaL_checktype(L, 2, LUA_TTABLE);
    auto buf = get_thread_encode_buffer();
    try {
        protobuf pb {};
        auto trace = protobuf::get_pb_trace();
        pb.encode_message(L, *buf, pb.find_message(cmd_name), cmd_name, trace, 0);
    } catch (const lua_protobuf_error& e) {
        luaL_error(L, e.what());
    }
    lua_pushlstring(L, buf->data(), buf->size());
    return 1;
}

int pb_decode(lua_State* L) {
    auto cmd_name = check_string_view(L, 1);
    auto data = check_string_view(L, 2);

    try {
        stream_reader stream { data.data(), data.size() };
        protobuf pb {};
        auto trace = protobuf::get_pb_trace();
        pb.decode_message(L, stream, pb.find_message(cmd_name), trace, 0);
    } catch (const lua_protobuf_error& e) {
        luaL_error(L, e.what());
    }
    return 1;
}

int pb_messages(lua_State* L) {
    auto descriptor = global_descriptor_ptr();
    lua_createtable(L, 0, (int)descriptor->messages.size());
    for (auto& [name, message]: descriptor->messages) {
        lua_pushlstring(L, name.data(), name.size());
        lua_pushlstring(L, message->name.data(), message->name.size());
        lua_rawset(L, -3);
    }
    return 1;
}

int pb_fields(lua_State* L) {
    auto full_name = check_string_view(L, 1);
    lua_createtable(L, 0, 16);
    protobuf pb {};
    if (auto message = pb.find_message(full_name); message) {
        for (auto& field: message->all_fields) {
            lua_pushstring(L, field.name.data());
            lua_pushinteger(L, (int)field.type);
            lua_rawset(L, -3);
        }
    }
    return 1;
}

int pb_enums(lua_State* L) {
    auto descriptor = global_descriptor_ptr();
    lua_createtable(L, (int)descriptor->enums.size(), 0);
    size_t i = 0;
    for (auto& [name, _]: descriptor->enums) {
        lua_pushstring(L, name.data());
        lua_rawseti(L, -2, ++i);
    }
    return 1;
}

extern "C" {
LUAMOD_API int luaopen_protobuf(lua_State* L) {
    luaL_Reg l[] = {
        { "load", load_pb },
        { "enums", pb_enums},
        { "messages", pb_messages },
        { "fields", pb_fields},
        { "encode", pb_encode },
        { "decode", pb_decode },
        { nullptr, nullptr },
    };
    luaL_newlib(L, l);
    return 1;
}
}