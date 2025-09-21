 ---@diagnostic disable: undefined-global
 
 add_lua_module("./", "protobuf", {
     all = function ()
        language "C++"
     end
 }, true) -- true means build as shared library