# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(['spr.py'],
             binaries = [
                ('C:\\Program Files\\Python37\\python37.dll', '.'),
                ('C:\\Windows\\System32\\vcruntime140.dll', '.')
             ],
             datas=[
                ('LICENSE', '.'),
                ('RevenewML/preprocessing/sql/*.sql', 'RevenewML/preprocessing/sql'),
                ('RevenewML/savedmodels/*.dat', 'RevenewML/savedmodels'),
             ],
             hiddenimports=['pyodbc','scipy','ipython','cython','pywin32','sklearn'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=True,
             win_private_assemblies=True,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          [],
          exclude_binaries=True,
          name='spr',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=False,
          console=False )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=False,
               upx_exclude=[],
               name='spr')
