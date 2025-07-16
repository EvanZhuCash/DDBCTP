# Building vnpy_ctp Package

This document provides simple instructions for building the vnpy_ctp C++ package.

## Prerequisites

Ensure you have the following installed:
- Python 3.13 (or compatible version)
- pip
- meson
- pybind11
- GCC compiler

Check if prerequisites are installed:
```bash
python3 --version
pip list | grep -E "(meson|pybind11)"
which meson
gcc --version
```

## Build Instructions

### Method 1: Standard Installation (Recommended)

1. Navigate to the vnpy_ctp directory:
```bash
cd vnpy_ctp
```

2. Clean any previous builds (optional):
```bash
pip uninstall vnpy_ctp -y
```

3. Build and install the package:
```bash
pip install . --verbose
```

### Method 2: Development Installation

For development work where you want changes to be reflected immediately:

```bash
cd vnpy_ctp
pip install -e . --verbose
```

## Verification

Test that the installation was successful:

```bash
# Change to a different directory to avoid import conflicts
cd /tmp

# Test imports
python3 -c "
import vnpy_ctp
from vnpy_ctp import CtpGateway
from vnpy_ctp.api import MdApi, TdApi
print('✅ All imports successful!')
print('✅ C++ modules loaded correctly')
"
```

## Troubleshooting

### Common Issues

1. **Import errors**: Make sure you're not running Python from the source directory
2. **Missing dependencies**: Install missing packages with pip
3. **Compiler errors**: Ensure GCC and development headers are installed

### Clean Build

If you encounter issues, try a clean build:

```bash
cd vnpy_ctp
pip uninstall vnpy_ctp -y
rm -rf build/ dist/ *.egg-info/
pip install . --verbose
```

## Build Output

Successful build should show:
- ✅ Compilation of vnctpmd.cpp and vnctptd.cpp
- ✅ Linking with CTP libraries (libthostmduserapi_se.so, libthosttraderapi_se.so)
- ✅ Creation of Python extension modules (.so files)
- ✅ Package installation completion

## Package Structure

After successful build, the package provides:
- `vnpy_ctp.CtpGateway` - Main gateway class
- `vnpy_ctp.api.MdApi` - Market data API
- `vnpy_ctp.api.TdApi` - Trading API
- All CTP constants (THOST_FTDC_*)

## Notes

- The build uses Meson build system with pybind11
- C++ standard: C++17
- Platform: Linux x86_64
- The package maintains compatibility with the original vnpy_ctp structure
- vnpy.event and other vnpy dependencies have been removed as per project requirements
