# -*- mode: python ; coding: utf-8 -*-

block_cipher = None


a = Analysis(['ranking.py'],
             pathex=['C:\\Users\\MichaelJohnson\\revenewcc'],
             binaries=[('c:/users/michaeljohnson/anaconda3/envs/revenew/python37.dll', '.')],
             datas=[
                ('LICENSE', '.'),
             	('commodity.pkl', '.'),
             	('crossref.pkl', '.'),
             	('scorecard.pkl', '.'),
             	('gooey/images/*.png', 'gooey/images'),
             	('gooey/images/*.ico', 'gooey/images'),
             	('gooey/images/*.gif', 'gooey/images'),
             	('gooey/languages/*.json', 'gooey/languages'),
             ],
             hiddenimports=['pyodbc', 'pywin32'],
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
          name='ranking',
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
               name='ranking')
